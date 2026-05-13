# ncp — Claude 项目指南

> Agent-First 的大规模文件复制工具，支持 DB 断点续传、结构化输出和多端存储。

## 1. 项目概览

ncp 是一个面向海量文件迁移的命令行工具，核心设计目标：

- **海量规模**：流水线架构（Walker → Replicator → DBWriter），内存占用与文件数量无关，已验证支持 1000万+ 文件。
- **精确断点续传**：每个文件的复制/校验状态持久化到 PebbleDB，随时中断、精确恢复。
- **Agent-First 输出**：结构化 NDJSON 事件流（`file_complete`、`file_metadata_complete`、`progress_summary`），供脚本和 Agent 消费。
- **多端存储**：本地文件系统、远程 ncp 服务器（`ncp://`）、阿里云 OSS（`oss://`）、腾讯云 COS（`cos://`）、华为云 OBS（`obs://`）。
- **复制-校验闭环**：支持 "先复制再校验" 和 "先校验再增量复制" 两种工作流。

### 支持的操作

| 命令 | 作用 |
|------|------|
| `ncp copy <src>... <dst>` | 复制文件（多源 → 单目标） |
| `ncp cksum <src> <dst>` | 校验源和目标的数据一致性 |
| `ncp resume <taskID>` | 自动检测 jobType 并恢复中断任务 |
| `ncp serve` | 启动 ncp 协议服务器（单 task、单客户端、单模式，task 完成后自动退出） |
| `ncp task list/show/delete` | 任务管理 |

---

## 2. 架构设计

### 2.1 核心流水线

```
Walker(1) ──discoverCh──→ Replicator(N) ──resultCh──→ DBWriter(1)
```

- **Walker**：遍历源目录，将发现的文件写入 PebbleDB（状态=discovered），并推送到 `discoverCh`。
- **Replicator**：N 个并行 worker，从 `discoverCh` 读取文件，执行复制或校验，将结果推送到 `resultCh`。
- **DBWriter**：单 goroutine，批量接收结果，更新 PebbleDB 状态，定期 flush。
- **反压机制**：当 channel 满时，Walker 只写 DB 不推 channel，遍历结束后 replay DB 中未完成的文件。

### 2.2 进度状态机

每个文件在 PebbleDB 中存储 2 字节：`[CopyStatus][CksumStatus]`

```
CopyStatus:  discovered(1) → dispatched(2) → done(3) / error(4)
CksumStatus: none(0) → pending(1) → pass(2) / mismatch(3) / error(4)
```

特殊键 `__walk_complete` 标记遍历是否完成，用于 resume 决策：
- 存在 `__walk_complete` → resume 时直接从 DB replay，不重走目录。
- 不存在 → 清空 DB，重新遍历。

### 2.3 Copy vs Cksum 的工作流

**场景1：先复制，后校验**
```bash
ncp copy /data/project /backup/project     # task-xxx 生成
ncp cksum --task task-xxx                   # 校验已复制文件
ncp copy --task task-xxx                    # 仅重传校验失败的文件
```

**场景2：先校验，后增量复制**
```bash
ncp cksum /data/project /backup/project     # 比对差异
cp copy --task task-xxx                     # 仅复制不匹配的文件
```

### 2.4 目录结构

```
cmd/ncp/              # 命令行入口（Composition Root）
  main.go             # Cobra 命令定义、DI 组装、resume 逻辑

pkg/
  interfaces/
    storage/          # 存储抽象（Source / Destination / Walker / Reader / Writer）
    progress/         # 进度存储抽象（ProgressStore / Batch / Iterator）
  model/              # 核心数据结构（DiscoverItem / FileResult / Status / Metadata）
  impls/
    storage/
      local/          # 本地文件系统实现（DirectIO、xattr、mtime 恢复）
      aliyun/         # 阿里云 OSS 实现
      remote/         # ncp:// 协议客户端实现
    progress/pebble/  # PebbleDB 进度存储实现

internal/
  copy/               # copy 命令的流水线（walker.go / replicator.go / dbwriter.go / job.go）
  cksum/              # cksum 命令的流水线（worker.go / dbwriter.go / job.go）
  protocol/           # ncp 网络协议（frame / message / conn / server / client）
  ncpserver/          # ncp serve 命令的服务端（单 task、walker DB、状态机）
  serve/              # (deprecated) 旧 connection handler
  filelog/            # 结构化日志（FileLog / ProgramLog）
  task/               # 任务元数据管理（meta.json / 文件锁）
  config/             # 配置加载（Viper + 分层配置文件）
  di/                 # 依赖注入工厂（storage / progress store 组装）
  signal/             # 信号处理

pkg/
  interfaces/
    storage/          # 存储抽象（Source / Destination / Walker / Reader / Writer）
    progress/         # 进度存储抽象（ProgressStore / Batch / Iterator）
    walkerdb/         # WalkerDB 抽象（目录遍历结果的持久化存储）
  model/              # 核心数据结构（DiscoverItem / FileResult / Status / Metadata）
  impls/
    storage/
      local/          # 本地文件系统实现（DirectIO、xattr、mtime 恢复）
      aliyun/         # 阿里云 OSS 实现
      cos/            # 腾讯云 COS 实现
      obs/            # 华为云 OBS 实现
      remote/         # ncp:// 协议客户端实现
    progress/pebble/  # PebbleDB 进度存储实现
    walkerdb/pebble/  # PebbleDB WalkerDB 实现

integration_test/     # 集成测试（本地↔OSS↔远程 交叉测试）
```

### 2.5 Path Semantics

**Copy:** `ncp copy` wraps all sources in `BasenamePrefixedSource`, so every source (single or multiple) is placed under its basename as a subdirectory of `dst`.

```
ncp copy /data/dir /tmp/           → /tmp/dir/...
ncp copy a b /tmp/                 → /tmp/a/..., /tmp/b/...
ncp copy oss://bucket/ /tmp/       → /tmp/bucket/...
```

**Cksum:** Both `src` and `dst` are explicit base paths. No automatic basename joining is performed.

```
ncp cksum /data/dir /tmp/dir       # compare /data/dir/* with /tmp/dir/*
```

---

## 3. 核心接口契约

### 3.1 存储接口（`pkg/interfaces/storage`）

```go
// Source = 可读端（Walker + Reader）
type Source interface {
    Walker                      // Walk(ctx, fn) 遍历
    Open(relPath) (Reader, error)   // 打开文件读取
    Restat(relPath) (DiscoverItem, error)
    Base() string
}

// Destination = 可写端
type Destination interface {
    OpenFile(ctx, relPath, size, mode, uid, gid) (Writer, error)
    Mkdir(ctx, relPath, mode, uid, gid) error
    Symlink(ctx, relPath, target) error
    SetMetadata(ctx, relPath, FileMetadata) error
}

// Writer = pwrite 语义
type Writer interface {
    WriteAt(p []byte, offset int64) (n int, err error)
    Sync() error
    Close(ctx context.Context, checksum []byte) error
}
```

**实现约束**：
- `local.Destination` 支持 DirectIO、SyncWrites、xattr、目录 mtime 恢复。
- `aliyun.Destination` 将 POSIX 元数据存为 OSS 自定义 header（`ncp-mode`、`ncp-uid` 等）。
- `remote.Destination` 通过 ncp 协议推送文件到远端服务器。

### 3.2 进度存储接口（`pkg/interfaces/progress`）

```go
type ProgressStore interface {
    Open(dir string) error
    Get(relPath) (CopyStatus, CksumStatus, error)
    Set(relPath, CopyStatus, CksumStatus) error
    Batch() Batch               // 批量写入
    Iter() (Iterator, error)    // 全量扫描
    Sync() error
    Close() error
    SetWalkComplete(total int64)
    HasWalkComplete() (bool, error)
    Reopen() error
    Destroy() error
}
```

---

## 4. 网络协议（ncp://）

MVP 阶段使用明文 TCP + 自研帧协议，仅适用于内网/VPN。

### 帧格式

```
+--------+---------+------+--------+--------+
| Magic  | Version | Type | Length | CRC32  |
| 4 bytes| 1 byte  |1 byte| 4 bytes| 4 bytes|
+--------+---------+------+--------+--------+
Magic   = 0x4E435004 ("NCP" + version bump)
Version = 2
Header  = 14 bytes
MaxPayload = 16 MB
```

### 消息类型

| Type | 名称 | 方向 | 说明 |
|------|------|------|------|
| 1 | Open | C→S | 打开/创建文件 |
| 2 | Pwrite | C→S | 写入数据块 |
| 3 | Fsync | C→S | 同步文件 |
| 4 | Close | C→S | 关闭文件，携带 checksum |
| 5 | Mkdir | C→S | 创建目录 |
| 6 | Symlink | C→S | 创建符号链接 |
| 7 | Utime | C→S | 设置时间 |
| 8 | Setxattr | C→S | 设置扩展属性 |
| 9 | TaskDone | C→S | **task 完成信号**（触发 serve 退出） |
| 10 | Init | C→S | 初始化连接（携带 Mode + TaskID + BasePath） |
| 11 | List | C→S | 请求目录列表（分页） |
| 12 | Pread | C→S | 读取文件数据 |
| 13 | Stat | C→S | 查询文件元数据 |
| 14 | AbortFile | C→S | 中止文件写入 |
| 0x81 | Ack | S→C | 确认 |
| 0x82 | Error | S→C | 错误响应 |
| 0x83 | Data | S→C | 数据响应（List 结果 / Pread 数据） |

---

## 5. 日志系统

ncp 只存在两种输出通道：**ProgramLog** 和 **FileLog**。除 `--help` 和命令行参数错误外，禁止任何直接输出到 stderr/stdout 的日志。

### 5.1 ProgramLog

记录程序内部运行是否正常的日志，供开发者/运维排查问题。通过 `--ProgramLogLevel` 控制级别，通过 `--ProgramLogOutput` 控制输出目标（`console` 或文件路径）。

| 级别 | 含义 |
|------|------|
| `fatal` | 系统级错误：内存不足、系统调用不支持、机器 arch 不匹配等，程序必须退出 |
| `error` | 程序遇到必须停下来的错误 |
| `warning` | 程序还能继续跑，但可能影响结果，用户应当阅读 |
| `info` | 程序性能方面的记录（吞吐量、速率等） |
| `debug` | 文件级流程是否正确（文件打开/关闭、跳过逻辑等） |
| `trace` | 文件内块级流程（每个 IO 块、offset、length 等） |

### 5.2 FileLog

通告给程序外部（用户或 AI Agent）的文件复制情况，是 ncp 的核心结构化输出。通过 `--enable-FileLog` 开关，通过 `--FileLogOutput` 控制输出目标（`console` 或文件路径），通过 `--FileLogInterval` 控制 `progress_summary` 的发出间隔。

所有事件为 NDJSON，包含 `timestamp`、`event`、`taskId` 字段。

#### `file_complete` — 文件内容级事件

指示单个文件的内容复制/校验是否完成、出错或跳过。**每个文件只产生一条。**

```json
{"timestamp":"...","event":"file_complete","taskId":"task-xxx","action":"copy","result":"done","relPath":"a/b.txt","fileType":"regular","fileSize":1024,"algorithm":"md5","checksum":"..."}
```

- `action`: `copy` 或 `cksum`
- `result`: `done` / `error` / `skipped`
- `skipped`: 当文件被 mtime/size/ETag 跳过时为 `true`
- cksum 模式下额外包含 `srcHash` 和 `dstHash`

#### `file_metadata_complete` — 文件元数据事件

指示单个文件的元数据（mode、uid、gid、mtime、xattr、symlink target 等）是否复制成功。**每个文件只产生一条。**

```json
{"timestamp":"...","event":"file_metadata_complete","taskId":"task-xxx","relPath":"a/b.txt","result":"done"}
```

- `result`: `done` / `error`
- `errorCode`: 失败时的错误信息

#### `progress_summary` — 作业级进度事件

反映整个复制/校验作业的进度、性能、内部计数器等指标，**定期发出**（默认 5 秒），作业完成时发出最终汇总（`finished=true`）。

```json
{"timestamp":"...","event":"progress_summary","taskId":"task-xxx","phase":"copy","finished":false,"exitCode":0,"walker":{...},"replicator":{...},"dbWriter":{...}}
```

- `phase`: `copy` 或 `cksum`
- `finished=true` 时为最终汇总
- `exitCode`: `0` = 全部成功，`2` = 存在错误/不匹配

---

## 6. 开发指南

### 6.1 构建与测试

```bash
make build          # 编译二进制到 ./ncp
make test           # 运行全部测试（unit + integration）
make unit           # 仅单元测试（含 race detector）
make integration    # 仅集成测试（需要 -tags=integration）
make lint           # golangci-lint
```

### 6.2 添加新的存储后端

1. 在 `pkg/impls/storage/<backend>/` 创建包。
2. 实现 `storage.Source`（如需作为源）和/或 `storage.Destination`（如需作为目标）。
3. 在 `internal/di/storage.go` 的 `NewSourceWithOSS` / `NewDestinationWithConfig` 中注册路径解析逻辑。
4. 添加单元测试和集成测试。

### 6.3 添加新的进度存储后端

1. 在 `pkg/impls/progress/<backend>/` 创建包。
2. 实现 `progress.ProgressStore` / `Batch` / `Iterator`。
3. 在 `internal/di/progress.go` 中提供工厂函数。

### 6.4 修改流水线行为

- **Walker**：`internal/copy/walker.go` — 控制遍历逻辑、resume replay、channel 反压。
- **Replicator**：`internal/copy/replicator.go` — 控制复制逻辑、skip-by-mtime、hasher 选择。
- **DBWriter**：`internal/copy/dbwriter.go` — 控制批量写入策略、flush 时机。
- **Cksum Worker**：`internal/cksum/worker.go` — 校验逻辑的独立实现。

### 6.5 配置系统

配置分层优先级（低到高）：
1. 代码默认值
2. `/etc/ncp_config.json`
3. `~/ncp_config.json`
4. `./ncp_config.json`
5. 环境变量（`NCP_` 前缀）
6. CLI flags

#### Profiles(云端凭据)

`ncp_config.json` 顶层 `Profiles` 字段定义云端凭据集,URL 通过 userinfo 引用:`oss://<profile>@bucket/path/`。

```json
{
  "Profiles": {
    "prod": {
      "Provider": "oss",
      "Endpoint": "oss-cn-shenzhen.aliyuncs.com",
      "Region":   "cn-shenzhen",
      "AK":       "${env:NCP_PROD_AK}",
      "SK":       "${env:NCP_PROD_SK}"
    }
  }
}
```

关键约束:
- 字段名 `AK`/`SK` 在配置面统一,各 backend 内部映射到 SDK 真实字段名(如 COS 的 SecretId/SecretKey)。
- `Provider` 必须等于 URL scheme(`oss://prod@...` 要求 `Profiles.prod.Provider == "oss"`),启动期立即校验。
- `${env:VAR}` 占位符在加载期解析,允许将明文密钥放在环境变量。
- 分层 config 间 profile 是**整体替换**(不字段级 merge),避免凭据半新半旧。
- 包含明文 `AK`/`SK`(非 `${env:...}` 引用)的 config 文件必须为 `0600`,否则 `config.CheckCredentialFilePermissions` 拒绝启动。
- 云 URL 缺 profile / 嵌入密码 / profile 在配置中找不到 → fail-fast,无任何回退路径。

### 6.6 代码规范

- **Go 版本**：1.23+
- **错误处理**：使用 `fmt.Errorf("...: %w", err)` 包装错误，不得静默丢弃。
- **并发安全**：共享状态通过 channel 传递，DB 操作集中在 DBWriter goroutine。
- **资源释放**：所有 `io.Closer` 使用 `defer` 释放，Pebble store 必须 `Close()`。
- **平台兼容**：OS-specific 代码使用 `_unix.go` / `_windows.go` / `_darwin.go` / `_linux.go` 后缀。
- **禁止直接打印**：除以下情况外，禁止任何直接输出到 stderr/stdout：
  1. `--help` 输出
  2. 命令行参数错误
  3. **查询命令的查询结果**（`task list`/`task show`/`task delete`、`config show`、`--dry-run` 等）：查询结果是 CLI 协议契约的一部分，以单行 JSON 写入 stdout，供脚本/Agent 消费。

  所有日志（诊断、进度）必须通过 ProgramLog（内部诊断）或 FileLog（外部通告）输出，不得使用 `fmt.Println`/`fmt.Printf`。

---

## 7. 集成测试

集成测试位于 `integration_test/`，需要 `-tags=integration` 才能运行：

| 测试文件 | 测试场景 |
|----------|----------|
| `copy_test.go` | 本地 → 本地复制 |
| `cksum_test.go` | 本地 ↔ 本地校验 |
| `oss_to_oss_test.go` | OSS → OSS |
| `oss_to_remote_test.go` | OSS → 远程 ncp |
| `remote_to_oss_test.go` | 远程 ncp → OSS |
| `remote_test.go` | 本地 → 远程 ncp（含断连恢复） |
| `aliyun_test.go` | 阿里云环境完整测试 |

测试环境通过 `testenv.go` 管理，需要配置阿里云 OSS 凭据（通过 `.env` 或环境变量）。

---

## 8. 常见陷阱

1. **OSS 必须用 profile 引用**:`oss://<profile>@bucket/path/`。profile 集中定义在 `ncp_config.json` 的 `Profiles` 下,`Profiles.<name>.Provider` 必须等于 URL scheme。`--cksum-algorithm` 必须为 `md5`(OSS 用 Content-MD5 校验,不支持 `xxh64`)。
2. **ncp:// 可作源或目标**：`ncp://` 既可以作为 copy/cksum 的 source，也可以作为 destination。
3. **多源限制**：多个本地源可以同时复制，但不能混用 `oss://` 或 `ncp://` 作为多源之一。
4. **copy vs cksum 路径语义不同**：`ncp copy /data/dir /tmp/` 产生 `/tmp/dir/...`（自动加 basename），而 `ncp cksum /data/dir /tmp/dir` 直接比对两端（无自动 join）。
5. **DirectIO 与 SyncWrites 互斥**：同时启用会在配置验证时报错。
5. **Resume 时 channelBuf 不可变**：resume 依赖 DB replay，channel buffer 大小在首次运行时固定。
6. **任务并发锁**：同一 taskID 不允许并发运行，通过文件锁保护（`task/lock_unix.go`）。
7. **远程协议无加密**：`ncp serve` 当前为明文传输，仅限可信网络使用。
8. **禁止直接打印到 stderr/stdout**：除 `--help`、参数错误、**查询命令的查询结果**（如 `task list/show/delete`、`config show`、`--dry-run`）外，任何 `fmt.Println`、`fmt.Fprintf(os.Stderr, ...)` 都是违规。应使用 ProgramLog（内部）或 FileLog（外部）。查询结果属于 CLI 协议契约，以单行 JSON 写入 stdout 供脚本/Agent 消费。
