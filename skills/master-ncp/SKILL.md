---
name: master-ncp
description: 'ncp 文件复制助手。通过交互式对话确定源/目标、确认所需 profile 已配置、执行复制并实时报告进度和最终结果。当用户需要复制文件、同步目录或迁移数据时使用。'
---

# master-ncp

**目标：** 通过交互式对话，为用户提供完整的 ncp 文件复制服务。

## ncp 知识库

执行任何步骤前，必须掌握以下 ncp 知识。

### 命令

| 命令 | 说明 |
|------|------|
| `ncp copy <src>... <dst>` | 从一个或多个源复制文件到目标。每个源会以其 basename 在 dst 下创建子目录 |
| `ncp cksum <src> <dst>` | 通过校验和比对验证数据一致性 |
| `ncp cksum --task <taskID>` | 验证已完成的复制任务 |
| `ncp resume <taskID>` | 恢复中断的复制或校验任务 |
| `ncp serve --base <dir> --listen <addr>` | 启动远程 ncp 服务器接收文件推送 |
| `ncp task list` | 列出所有任务 |
| `ncp task show <taskID>` | 查看任务详情 |
| `ncp task delete <taskID>` | 删除任务 |
| `ncp task migrate-profile <taskID> --profile <name>` | 为存量任务的 src/dst URL 注入 profile(profile 改造前的 task 升级后必须先迁移再 resume) |
| `ncp profile list` / `ncp profile show <name>` | 查看 profile

### 后端协议

| 协议 | 示例 | 说明 |
|------|------|------|
| 本地（无前缀） | `/data/project` | 可作为源和目标 |
| `ncp://` | `ncp://server:9900/backup` | 远程 ncp 服务器（仅目标） |
| `oss://` | `oss://prod@my-bucket/backup/` | 阿里云 OSS（可作为源和目标）。**必须**带 `<profile>@` 前缀 |
| `cos://` | `cos://prod@my-bucket-1250000000/backup/` | 腾讯云 COS（可作为源和目标）。**必须**带 `<profile>@` 前缀 |

**约束：**
- 源支持：本地、`oss://`、`cos://`
- 目标支持：本地、`ncp://`、`oss://`、`cos://`
- `ncp cksum` 不支持 `ncp://` 目标（协议层已有内置 MD5 校验）
- `oss://` 和 `cos://` 不带 profile / profile 在配置中找不到 → ncp 启动期立即报错。本地路径与 `ncp://` URL 不能带 userinfo。
- `oss://` 和 `cos://` 不能混用作为多源

### 关键参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--CopyParallelism` | 1 | 并行 worker 数量 |
| `--ChannelBuf` | 100000 | discover/result channel 缓冲区大小 |
| `--cksum-algorithm` | md5 | 校验算法：`md5` 或 `xxh64` |
| `--skip-by-mtime` | true | 跳过 mtime+size 一致的文件（OSS 还比较 ETag） |
| `--no-skip-by-mtime` | false | 禁用跳过，复制/校验所有文件 |
| `--ProgressStorePath` | /tmp/ncp_progress_store | 进度存储目录 |
| `--FileLogOutput` | /tmp/ncp_file_log.json | FileLog 输出路径（或 `console` 输出到 stdout） |
| `--FileLogInterval` | 5 | 进度摘要间隔（秒） |
| `--enable-SyncWrites` | true | 写入时启用 fsync |
| `--enable-DirectIO` | false | 启用 Direct IO（与 SyncWrites 互斥） |
| `--dry-run` | false | 打印生效配置后退出 |

> 云凭据**不再通过 CLI flag 传递**。改为在 `ncp_config.json` 的 `Profiles` 字段集中定义,URL 通过 `oss://<profile>@bucket/path/` 或 `cos://<profile>@bucket/path/` 引用。详见下文"Cloud Profiles"。

### Cloud Profiles

`oss://` 和 `cos://` URL 必须带 `<profile>@` 前缀,profile 在 `ncp_config.json` 顶层 `Profiles` 字段定义:

```json
{
  "Profiles": {
    "oss-prod": {
      "Provider": "oss",
      "Endpoint": "oss-cn-shenzhen.aliyuncs.com",
      "Region":   "cn-shenzhen",
      "AK":       "${env:NCP_PROD_AK}",
      "SK":       "${env:NCP_PROD_SK}"
    },
    "cos-prod": {
      "Provider": "cos",
      "Region":   "ap-guangzhou",
      "AK":       "${env:NCP_COS_AK}",
      "SK":       "${env:NCP_COS_SK}"
    }
  }
}
```

- 配置文件路径(分层加载,后层整体替换):`/etc/ncp_config.json` → `~/ncp_config.json` → `./ncp_config.json`。
- `oss://` URL 要求 `Provider == "oss"`; `cos://` URL 要求 `Provider == "cos"`。
- `${env:VAR}` 占位符在加载期解析。明文 AK/SK 要求 config 文件 `0600`,否则 ncp 拒绝启动。
- COS 的 `Endpoint` 可选。如不填写,自动构造为 `https://<bucket>.cos.<region>.myqcloud.com`。
- 跨账号/跨区域:为每个账号定义独立 profile,URL 各自带 `<profile>@`,如:
  ```bash
  ncp copy oss://acct-a@bkt-a/data/ oss://acct-b@bkt-b/data/
  ncp copy cos://acct-a@src-bucket/data/ cos://acct-b@dst-bucket/backup/
  ```
- 查看现有 profile:`ncp profile list`、`ncp profile show <name>`(AK/SK 输出脱敏)。
- 存量任务(profile 改造前已存在的 task)需要先用 `ncp task migrate-profile <taskID> --profile <name>` 迁移再 resume。

### 性能参数调优

CopyParallelism 和 ChannelBuf 直接影响 ncp 的内存使用和吞吐量，必须根据场景合理设置。

#### 内存消耗参考

| 组件 | 每份内存 | 份数 | 说明 |
|------|---------|------|------|
| discoverCh + resultCh 缓冲 | ~864B × N × 2 | 1 | 含 string 数据约 864B/条 |
| IO buf (replicator) | 128KB~4MB | CopyParallelism | 按 IOSizeTier 自动分配 |
| Cloud smallFileWriter buf (OSS) | ≤5MB | CopyParallelism | 小文件全量 buffer |
| Cloud partBuf (OSS) | 5MB | CopyParallelism | 分片上传缓冲 |
| Cloud smallFileWriter buf (COS) | ≤1MB | CopyParallelism | 小文件全量 buffer |
| Cloud partBuf (COS) | 1MB | CopyParallelism | 分片上传缓冲（COS 最小 1MB） |
| ncp:// TCP 连接 | ~几十KB | CopyParallelism | 每个 worker 一条连接 |

**ChannelBuf 内存估算**（两个 channel 合计）：

| ChannelBuf | 总内存占用 |
|------------|-----------|
| 100,000 | ~165 MB |
| 500,000 | ~825 MB |
| 1,000,000 | ~1.6 GB |
| 2,000,000 | ~3.3 GB |

#### CopyParallelism 推荐值

| 源介质 | 目标介质 | 文件数 < 1K | 文件数 ≥ 1K | 最大值 |
|--------|---------|-------------|-------------|--------|
| SSD | SSD | 2 | 4~cpu×2 | cpu×2 |
| SSD | HDD/网络/OSS | 2 | 2~4 | 4 |
| HDD | 任意 | 2 | 2 | 2 |
| 任意 | ncp:// | 2 | 4~8 | 8 |

**理由：**
- SSD 随机 IO 能力强，ncp 流水线中有 MD5 计算等 IO 间隙，高并行能填满间隙，上限取 cpu×2
- HDD 随机 IO 差，多 worker 只增加磁头寻道，2 个足够
- 网络目标（ncp/OSS）瓶颈在带宽，4~8 够用
- 小文件数 < 1K 时瓶颈不在并行，2 个足够保持流水线

#### ChannelBuf 推荐值

| 可用内存 | ChannelBuf | 说明 |
|----------|------------|------|
| < 2GB | 100,000 | ~165MB，安全 |
| 2~4GB | 500,000 | ~825MB |
| 4~16GB | 1,000,000 | ~1.6GB |
| > 16GB | 2,000,000 | ~3.3GB，千万级文件需要 |

**ChannelBuf 的意义：** 千万级文件场景下 Walker 生产速度远快于 Replicator。channel 缓冲太小时 Walker 被阻塞写 DB（back-pressure），walk 完成后需从 DB 回放。缓冲够大时 Walker 可直接通过 channel 投递，减少 DB 读写开销。

#### 其他性能参数

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| IOSize | 0 (tiered) | 分级自适应（128KB/1MB/4MB），无需手动调 |
| skip-by-mtime | true (默认) | 增量同步跳过已复制文件；全量复制用 --no-skip-by-mtime |
| cksum-algorithm | md5 | OSS 场景强制 md5；本地/ncp 可选 xxh64 提速 |

### FileLog 事件

ncp 输出结构化 NDJSON 事件，用于监控进度。

**`file_complete`** — 普通文件内容复制完成时输出（内容级事件）：
```json
{"timestamp":"...","event":"file_complete","taskId":"task-xxx","action":"copy","result":"done|error|skipped","relPath":"...","fileSize":N,"algorithm":"md5","checksum":"..."}
```

**`file_metadata_complete`** — 每个文件的元数据操作完成时输出（目录/符号链接只输出此事件）：
```json
{"timestamp":"...","event":"file_metadata_complete","taskId":"task-xxx","result":"done|error","relPath":"...","fileType":"regular|dir|symlink"}
```

**`progress_summary`** — 每 `FileLogInterval` 秒输出一次：
```json
{
  "event": "progress_summary",
  "phase": "copy",
  "finished": false,
  "exitCode": 0,
  "walker": {"walkComplete": true, "discoveredCount": 1000000, "dispatchedCount": 500000},
  "replicator": {"filesCopied": 480000, "bytesCopied": 107374182400, "filesPerSec": 3200.5, "bytesPerSec": 715827882.7},
  "dbWriter": {"totalDone": 480000, "totalFailed": 3, "totalProcessed": 480003}
}
```

`finished=true` 表示任务完成。`exitCode` 0 = 成功，2 = 有错误/不匹配。

### 退出码

| 退出码 | 含义 |
|--------|------|
| 0 | 所有文件复制/校验成功 |
| 2 | 部分文件失败或不匹配 |

## 工作流概览

分为三个步骤：

1. **收集需求** — 交互式对话确定源、目标和参数
2. **准备与执行** — 构建 ncp 命令、用户确认、执行、监控进度
3. **报告结果** — 输出最终结果并提供后续操作建议

## 交互规则

- **主动建议，而非盘问。** 提供合理默认值和推荐，而非逐一追问。只在答案不明确或意图模糊时才提问。
- **执行前确认。** 始终展示完整的 `ncp copy` 命令并获用户批准后再执行。
- **人性化报告进度。** 转换原始数字：`1073741824 bytes` → `1.0 GB`，`3200.5 filesPerSec` → `3,201 文件/秒`。
- **优雅处理错误。** ncp 退出码为 2 或进程失败时，清晰解释问题并建议后续操作（resume、cksum 等）。
- **保留 taskID。** 始终从输出中提取并记住 taskID — resume、cksum 和任务管理都需要它。

## 执行

完整阅读并遵循：`./steps/step-01-gather-requirements.md`
