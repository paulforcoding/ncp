# ncp

面向 Agent 的大规模文件复制工具。

ncp 将文件复制到远程服务器和云对象存储，具备 DB 持久化进度追踪、结构化 Agent-First 输出和精确断点续传能力。

## 特性

- **大规模复制** — 经过千万级文件测试。管道架构（Walker → Replicator → DBWriter）保持内存恒定。
- **DB 持久化进度** — 每个文件的复制/校验状态持久化到 PebbleDB。随时中断，恢复时精确续传。
- **Agent-First 输出** — 结构化 JSON FileLog 事件（`copy_plan`、`copy_progress`、`copy_complete`、`cksum_complete`），专为程序化消费设计。
- **多后端支持** — 本地文件系统、远程 ncp 服务器（`ncp://`）、阿里云 OSS（`oss://`）。
- **多源复制** — `ncp copy src1 src2 dst/` 将每个源复制到目标目录下对应的子目录。
- **数据校验** — 独立 `ncp cksum` 命令，支持 MD5 或 xxHash 算法。支持 copy→cksum→copy 循环。
- **文件时间戳保留** — 复制时保留普通文件的修改时间；目录时间戳在复制完成后统一恢复。
- **协议完整性** — 远程协议（ncp://）每帧携带 CRC32 校验，检测传输中的数据损坏。

## 安装

```bash
make build
# 二进制文件: ./ncp
```

## 快速开始

```bash
# 复制本地目录到另一个本地路径
ncp copy /data/project /backup/project

# 多源复制到一个目标目录
ncp copy /data/logs /data/configs /backup/

# 复制到远程 ncp 服务器
ncp serve --base /backup --listen :9900 &  # 在目标服务器上启动
ncp copy /data/project ncp://server:9900/backup/project

# 复制到阿里云 OSS
ncp copy /data/project oss://my-bucket/backup/ \
  --endpoint oss-cn-shenzhen.aliyuncs.com \
  --region cn-shenzhen \
  --access-key-id YOUR_AK \
  --access-key-secret YOUR_SK

# 校验数据一致性
ncp cksum /data/project /backup/project
ncp cksum /data/project oss://my-bucket/backup/ --endpoint ... --region ...

# 恢复中断的任务
ncp resume task-20260502-143000-abcd

# 使用特定命令恢复
ncp copy --task task-20260502-143000-abcd
```

## 命令

### `ncp copy <src>... <dst>`

从一个或多个源复制文件到目标。支持本地、`ncp://` 和 `oss://` 协议。

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--CopyParallelism` | 1 | 并行复制工作线程数 |
| `--IOSize` | 0（分层） | IO 大小（字节） |
| `--cksum-algorithm` | md5 | 校验算法：`md5` 或 `xxh64` |
| `--enable-DirectIO` | false | 启用 Direct IO（与 SyncWrites 互斥） |
| `--enable-SyncWrites` | true | 启用写 fsync |
| `--enable-EnsureDirMtime` | true | 复制后恢复目录修改时间 |
| `--enable-FileLog` | true | 启用结构化 FileLog 输出 |
| `--FileLogOutput` | console | FileLog 输出：console 或文件路径 |
| `--FileLogInterval` | 5 | FileLog 输出间隔（秒） |
| `--ProgressStorePath` | ./progress | 进度存储目录 |
| `--ProgramLogLevel` | info | 日志级别：trace/debug/info/warn/error/critical |
| `--dry-run` | false | 打印有效配置后退出 |
| `--task` | | 按 taskID 恢复已有任务 |
| `--endpoint` | | OSS endpoint |
| `--region` | | OSS 区域 |
| `--access-key-id` | | OSS AccessKey ID |
| `--access-key-secret` | | OSS AccessKey Secret |

### `ncp cksum <src> <dst>`

通过比对校验和验证源端与目的端数据一致性。

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--cksum-algorithm` | md5 | 校验算法：`md5` 或 `xxh64` |
| `--CopyParallelism` | 1 | 并行校验工作线程数 |
| `--task` | | 按 taskID 恢复已有校验任务 |

### `ncp resume <taskID>`

恢复中断的复制或校验任务。自动检测上次运行的任务类型。

### `ncp serve`

启动 ncp 协议服务器接收文件推送。

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--listen` | :9900 | 监听地址 |
| `--base` | | 接收文件的基目录（必填） |

### `ncp task`

管理任务：`list`、`show <taskID>`、`delete <taskID>`。

## 架构

```
Walker(1) ──discoverCh──→ Replicator(N) ──resultCh──→ DBWriter(1)
```

- **Walker** 遍历源端目录，将进度写入 PebbleDB，推送项目到发现通道。
- **Replicator**（N 个工作线程）从源端复制文件到目的端，流式计算校验和。
- **DBWriter** 批量处理结果并持久化到 PebbleDB。
- **背压机制**：通道满时，Walker 仅写 DB，遍历完成后回放。

进度以 2 字节值 `[CopyStatus][CksumStatus]` 存储在以相对路径为键的 DB 中，使用 `__walk_complete` 标记判断续传策略。

## 复制-校验-重试工作流

```
1. ncp copy /src /dst              # 复制所有文件
2. ncp cksum --task <taskID>       # 校验已复制文件
3. ncp copy --task <taskID>        # 只重新复制校验不通过的文件
```

步骤 3 仅重新复制 `cksumStatus != pass` 的文件，因为 `ResumeFromDB` 会跳过 `CksumPass` 或 `CopyDone+CksumNone` 的文件。

## 开发

```bash
make build          # 构建二进制
make test           # 运行所有测试
make unit           # 仅单元测试
make integration    # 仅集成测试
make lint           # 运行 golangci-lint
```

## 许可证

MIT
