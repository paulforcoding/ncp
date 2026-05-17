# ncp copy

## 触发条件

当以下情况出现时读这个文件：
- 需要执行文件复制
- 不确定 copy 命令的参数和约束
- 需要理解 copy 的路径语义
- 用户要求复制文件/目录/数据迁移

## 快速行动

**首次运行**（新建 task）：
```bash
ncp copy <src>... <dst> \
  --FileLogOutput /tmp/ncp_file.log --FileLogInterval 10 \
  --ProgramLogOutput /tmp/ncp_program.log --ProgramLogLevel warn
```

**后续运行**（基于已有 task，如 cksum 后重传 mismatch 文件、resume 中断任务）：
```bash
ncp copy --task <taskID> \
  --FileLogOutput /tmp/ncp_file.log --FileLogInterval 10 \
  --ProgramLogOutput /tmp/ncp_program.log --ProgramLogLevel warn
```

两种模式的区别：首次运行必须提供 `<src>... <dst>`，`--task` 模式从 meta.json 读取 src/dst，不需要再传路径。执行后立即监控 FileLog 中的 `progress_summary` 事件，记录 taskId。

## 详情

### 路径语义（对齐 cp）

| 条件 | 行为 | 示例 |
|------|------|------|
| 单源 + dst 不存在 | 复制 AS dst（不加 basename 前缀） | `ncp copy /data/dir /tmp/newname` → `/tmp/newname/...` |
| 单源 + dst 已存在且为目录 | 复制 INTO dst（加 basename 前缀） | `ncp copy /data/dir /tmp/existing` → `/tmp/existing/dir/...` |
| 多源 + dst 已存在且为目录 | 复制 INTO dst（每个源加 basename 前缀） | `ncp copy a b /tmp/existing` → `/tmp/existing/a/...`, `/tmp/existing/b/...` |
| 多源 + dst 不存在 | **报错** | — |

### 完整 flags

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--CopyParallelism` | 2 | 并行 worker 数量 |
| `--ChannelBuf` | 100000 | discover/result channel 缓冲区大小 |
| `--FileLogOutput` | console | FileLog 输出：console 或文件路径 |
| `--FileLogInterval` | 5 | progress_summary 输出间隔（秒） |
| `--ProgramLogOutput` | console | ProgramLog 输出：console 或文件路径 |
| `--ProgramLogLevel` | info | 日志级别：trace/debug/info/warn/error/critical |
| `--ProgressStorePath` | /tmp/ncp_progress_store | 进度存储目录 |
| `--cksum-algorithm` | md5 | 校验算法：md5 或 xxh64 |
| `--skip-by-mtime` | true | 跳过 mtime+size 一致的文件（OSS 还比较 ETag） |
| `--no-skip-by-mtime` | false | 禁用跳过，复制所有文件 |
| `--enable-FileLog` | true | 启用 FileLog |
| `--disable-FileLog` | false | 禁用 FileLog |
| `--enable-SyncWrites` | true | 写入时启用 fsync（与 DirectIO 互斥） |
| `--disable-SyncWrites` | false | 禁用 fsync |
| `--enable-DirectIO` | false | 启用 Direct IO（与 SyncWrites 互斥） |
| `--disable-DirectIO` | true | 禁用 Direct IO |
| `--enable-EnsureDirMtime` | true | 复制后恢复目录 mtime |
| `--disable-EnsureDirMtime` | false | 不恢复目录 mtime |
| `--IOSize` | 0 | IO 块大小（0 = 分级自适应：128KB/1MB/4MB） |
| `--dry-run` | false | 预览生效配置后退出，不执行复制 |
| `--task` | "" | 指定 taskID 继续（后续运行模式，不需要 `<src> <dst>`） |

### 多源限制

- 多个本地源可以同时复制
- 不能混用 `oss://`、`cos://`、`obs://`、`ncp://` 作为多源之一

### CopyParallelism 推荐

| 源介质 | 目标介质 | 推荐值 | 最大值 |
|--------|---------|--------|--------|
| SSD | SSD | 4~cpu×2 | cpu×2 |
| SSD | HDD/网络/ncp/云 | 2~4 | 4 |
| HDD | 任意 | 2 | 2 |

### ChannelBuf 推荐

| 可用内存 | ChannelBuf | 约占内存 |
|----------|------------|---------|
| < 2GB | 100000 | ~165MB |
| 2~4GB | 500000 | ~825MB |
| 4~16GB | 1000000 | ~1.6GB |
| > 16GB | 2000000 | ~3.3GB |

### cksum-algorithm 约束

- OSS/COS/OBS 场景必须用 `md5`（云存储用 Content-MD5 校验，不支持 xxh64）
- 本地和 ncp:// 场景可选 `xxh64` 提速

### SyncWrites 与 DirectIO 互斥

同时启用 `--enable-SyncWrites` 和 `--enable-DirectIO` 会在启动时报错。二者只能选其一或都不选（默认 SyncWrites=true）。

## 关联文件

- serve.md — 目标为 ncp:// 时需先启动 serve
- url-schemes.md — 各存储后端的 URL 格式
- profiles.md — 云存储凭据配置
- log.md — 三条信息通道与监控方案
