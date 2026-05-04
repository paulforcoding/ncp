---
name: master-ncp
description: 'ncp 文件复制助手。通过交互式对话确定源/目标、配置 OSS 凭据、执行复制并实时报告进度和最终结果。当用户需要复制文件、同步目录或迁移数据时使用。'
---

# master-ncp

**目标：** 通过交互式对话，为用户提供完整的 ncp 文件复制服务。

## ncp 知识库

执行任何步骤前，必须掌握以下 ncp 知识。

### 命令

| 命令 | 说明 |
|------|------|
| `ncp copy <src>... <dst>` | 从一个或多个源复制文件到目标 |
| `ncp cksum <src> <dst>` | 通过校验和比对验证数据一致性 |
| `ncp cksum --task <taskID>` | 验证已完成的复制任务 |
| `ncp resume <taskID>` | 恢复中断的复制或校验任务 |
| `ncp serve --base <dir> --listen <addr>` | 启动远程 ncp 服务器接收文件推送 |
| `ncp task list` | 列出所有任务 |
| `ncp task show <taskID>` | 查看任务详情 |
| `ncp task delete <taskID>` | 删除任务 |

### 后端协议

| 协议 | 示例 | 说明 |
|------|------|------|
| 本地（无前缀） | `/data/project` | 可作为源和目标 |
| `ncp://` | `ncp://server:9900/backup` | 远程 ncp 服务器（仅目标） |
| `oss://` | `oss://my-bucket/backup/` | 阿里云 OSS（可作为源和目标） |

**约束：**
- 源支持：本地、`oss://`
- 目标支持：本地、`ncp://`、`oss://`
- `ncp cksum` 不支持 `ncp://` 目标（协议层已有内置 MD5 校验）

### 关键参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--CopyParallelism` | 1 | 并行 worker 数量 |
| `--ChannelBuf` | 100000 | discover/result channel 缓冲区大小 |
| `--cksum-algorithm` | md5 | 校验算法：`md5` 或 `xxh64` |
| `--skip-by-mtime` | true | 跳过 mtime+size 一致的文件（OSS 还比较 ETag） |
| `--no-skip-by-mtime` | false | 禁用跳过，复制/校验所有文件 |
| `--endpoint` | | OSS endpoint（如 `oss-cn-shenzhen.aliyuncs.com`） |
| `--region` | | OSS 区域（如 `cn-shenzhen`） |
| `--access-key-id` | | OSS AccessKey ID |
| `--access-key-secret` | | OSS AccessKey Secret |
| `--ProgressStorePath` | /tmp/ncp_progress_store | 进度存储目录 |
| `--FileLogOutput` | /tmp/ncp_file_log.json | FileLog 输出路径（或 `console` 输出到 stdout） |
| `--FileLogInterval` | 5 | 进度摘要间隔（秒） |
| `--enable-SyncWrites` | true | 写入时启用 fsync |
| `--enable-DirectIO` | false | 启用 Direct IO（与 SyncWrites 互斥） |
| `--dry-run` | false | 打印生效配置后退出 |

### 性能参数调优

CopyParallelism 和 ChannelBuf 直接影响 ncp 的内存使用和吞吐量，必须根据场景合理设置。

#### 内存消耗参考

| 组件 | 每份内存 | 份数 | 说明 |
|------|---------|------|------|
| discoverCh + resultCh 缓冲 | ~864B × N × 2 | 1 | 含 string 数据约 864B/条 |
| IO buf (replicator) | 128KB~4MB | CopyParallelism | 按 IOSizeTier 自动分配 |
| OSS smallFileWriter buf | ≤5MB | CopyParallelism | 小文件全量 buffer |
| OSS partBuf | 5MB | CopyParallelism | 分片上传缓冲 |
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

**`copy_plan`** — 任务启动时输出一次：
```json
{"timestamp":"...","event":"copy_plan","taskId":"task-xxx","sources":["/src"],"dest":"/dst","algorithm":"md5"}
```

**`file_complete`** — 每个文件完成时输出：
```json
{"timestamp":"...","event":"file_complete","taskId":"task-xxx","action":"copy","result":"done|error","relPath":"...","fileSize":N,"algorithm":"md5","checksum":"...","skipped":true}
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
