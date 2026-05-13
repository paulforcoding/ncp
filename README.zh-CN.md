# ncp

面向 AI Agent 的、支持断点续传的大规模文件复制工具。

ncp 将文件复制到远程服务器和云对象存储，具备 DB 持久化进度追踪、结构化 Agent-First 输出和精确断点续传能力。

## 特性

- **大规模复制** — 经过千万级文件测试。管道架构（Walker → Replicator → DBWriter）保持内存恒定。
- **使用 db 记录文件复制进度** — 每个文件的复制/校验状态持久化到 PebbleDB。随时中断，恢复时精确续传。
- **高性能** — 在提供 DB 记录级断点续传的功能下，尽量提高性能，减少 db 对复制的影响
- **独创用法** — 既可以先做 cksum 校验，基于校验结果做增量复制；也支持先复制，在做数据校验确保数据一致
- **Agent-First 输出** — 结构化 NDJSON FileLog 事件（`file_complete`、`file_metadata_complete`、`progress_summary`），专为 Agent 和脚本程序化消费设计。
- **多后端支持** — 本地文件系统、远程 ncp 服务器（`ncp://`）、阿里云 OSS（`oss://`）、腾讯云 COS（`cos://`）、华为云 OBS（`obs://`）。
- **数据校验** — 独立 `ncp cksum` 命令，支持 MD5 或 xxHash 算法。支持 copy→cksum→copy 循环。

## 注意
- 本项目只支持普通文件、目录和符号链接的复制。不支持管道、socket 文件、设备文件等特殊文件
- 本项目只支持的元数据包括：mode、owner、mtime、xattr，其他诸如 acl 等特殊属性不支持

## 安装

```bash
make build
# 二进制文件: ./ncp
```

## 快速开始

```bash
# 复制本地目录 — 结果在 /backup/project/...
ncp copy /data/project /backup/

# 多源复制到一个目标目录
ncp copy /data/logs /data/configs /backup/

# 复制到远程 ncp 服务器 — 在服务器上创建 /backup/project/...
ncp serve --listen :9900   # 在目标服务器上启动
ncp copy /data/project ncp://server:9900/backup/

# 从远程 ncp 服务器复制 — 从服务器读取 /data/project
ncp serve --listen :9900   # 在源服务器上启动
ncp copy ncp://server:9900/data/project /backup/

# 复制到阿里云 OSS — 在 bucket 下创建 backup/project/...
ncp copy /data/project oss://prod@my-bucket/backup/

# 复制整桶 OSS — 结果在 /restore/my-bucket/...
ncp copy oss://prod@my-bucket/ /restore/

# 复制到腾讯云 COS — 在 bucket 下创建 backup/project/...
ncp copy /data/project cos://cos-prod@my-bucket-1250000000/backup/

# 复制整桶 COS — 结果在 /restore/my-bucket/...
ncp copy cos://cos-prod@my-bucket-1250000000/ /restore/

# 复制到华为云 OBS — 在 bucket 下创建 backup/project/...
ncp copy /data/project obs://obs-prod@my-bucket/backup/

# 复制整桶 OBS — 结果在 /restore/my-bucket/...
ncp copy obs://obs-prod@my-bucket/ /restore/

# 校验数据一致性（两端都是显式基址）
ncp cksum /data/project /backup/project
ncp cksum /data/project oss://prod@my-bucket/backup/project
ncp cksum /data/project cos://cos-prod@my-bucket-1250000000/backup/project
ncp cksum /data/project obs://obs-prod@my-bucket/backup/project

# 恢复中断的任务
ncp resume task-20260502-143000-abcd

# 使用特定命令恢复
ncp copy --task task-20260502-143000-abcd
```

## 命令

### `ncp copy <src>... <dst>`

从一个或多个源复制文件到目标。支持本地、`ncp://`、`oss://`、`cos://` 和 `obs://` 协议。

**路径语义：** 每个源都会以其 basename 在 `dst` 下创建子目录。单源和多源均遵循此规则。

```
ncp copy /data/project /backup/
# 结果：/backup/project/...

ncp copy oss://prod@my-bucket/ /restore/
# 结果：/restore/my-bucket/...
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--CopyParallelism` | 1 | 并行复制工作线程数 |
| `--IOSize` | 0（分层） | IO 大小（字节） |
| `--cksum-algorithm` | md5 | 校验算法：`md5` 或 `xxh64` |
| `--enable-DirectIO` | false | 启用 Direct IO（与 SyncWrites 互斥） |
| `--enable-SyncWrites` | true | 启用写 fsync |
| `--enable-EnsureDirMtime` | true | 复制后恢复目录修改时间 |
| `--enable-FileLog` | true | 启用结构化 FileLog 输出 |
| `--FileLogOutput` | /tmp/ncp_file_log.json | FileLog 输出：console 或文件路径 |
| `--FileLogInterval` | 5 | FileLog progress_summary 输出间隔（秒） |
| `--ProgressStorePath` | /tmp/ncp_progress_store | 进度存储目录 |
| `--ProgramLogLevel` | info | 日志级别：trace/debug/info/warn/error/critical |
| `--dry-run` | false | 打印有效配置后退出 |
| `--task` | | 按 taskID 恢复已有任务 |

### `ncp cksum <src> <dst>`

通过比对校验和验证源端与目的端数据一致性。`src` 和 `dst` 都是显式基址，不会自动 join basename。

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--cksum-algorithm` | md5 | 校验算法：`md5` 或 `xxh64` |
| `--CopyParallelism` | 1 | 并行校验工作线程数 |
| `--task` | | 按 taskID 恢复已有校验任务 |

### `ncp resume <taskID>`

恢复中断的复制或校验任务。自动检测上次运行的任务类型。

### `ncp serve`

启动 ncp 协议服务器，为单次复制/校验任务服务。服务器只服务一个客户端、一个任务、一种模式（Source 或 Destination）。任务完成后收到 `MsgTaskDone` 信号即退出。

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--listen` | :9900 | 监听地址 |
| `--serve-temp-dir` | /tmp/ncpserve | walker DB 临时目录（Source 模式使用） |

### `ncp task`

管理任务：`list`、`show <taskID>`、`delete <taskID>`。

### `ncp config`

查看合并后的生效配置。AK/SK 自动脱敏。

```bash
ncp config show                     # 展示全部生效配置
ncp config show --profile <name>    # 只展示指定 profile
```

所有执行命令（`copy`、`cksum`、`resume`）都支持 `--dry-run`，可在不执行的情况下预览生效配置，并标注给定 URL 会使用哪些 profile。

```bash
ncp copy  /data/dir  oss://prod@bucket/backup  --dry-run
ncp cksum /data/dir  oss://prod@bucket/backup  --dry-run
ncp resume task-xxx --dry-run
```

## Profiles(云端凭据)

云 URL 通过 userinfo 引用名为 profile 的凭据集:`oss://<profile>@bucket/path/`、`cos://<profile>@bucket/path/` 或 `obs://<profile>@bucket/path/`。Profile 集中定义在 `ncp_config.json` 的 `Profiles` 字段下。同一 profile 名可以指向不同账号或区域,这正是跨账号迁移的表达方式:

```bash
# 跨账号云存储:两端各自选择 profile
ncp copy oss://acct-a@bkt-a/data/ oss://acct-b@bkt-b/data/
ncp copy cos://acct-a@src-bucket/data/ cos://acct-b@dst-bucket/backup/
ncp copy obs://acct-a@src-bucket/data/ obs://acct-b@dst-bucket/backup/
```

`ncp_config.json` 走分层加载链(`/etc/ncp_config.json` → `~/ncp_config.json` → `./ncp_config.json`),后层会**整个替换**前层中同名 profile(不做字段级合并,凭据不会半新半旧)。

```json
{
  "Profiles": {
    "prod": {
      "Provider": "oss",
      "Endpoint": "oss-cn-shenzhen.aliyuncs.com",
      "Region":   "cn-shenzhen",
      "AK":       "${env:NCP_PROD_AK}",
      "SK":       "${env:NCP_PROD_SK}"
    },
    "dr": {
      "Provider": "oss",
      "Endpoint": "oss-cn-beijing.aliyuncs.com",
      "Region":   "cn-beijing",
      "AK":       "${env:NCP_DR_AK}",
      "SK":       "${env:NCP_DR_SK}"
    },
    "cos-prod": {
      "Provider": "cos",
      "Region":   "ap-guangzhou",
      "AK":       "${env:NCP_COS_AK}",
      "SK":       "${env:NCP_COS_SK}"
    },
    "obs-prod": {
      "Provider": "obs",
      "Endpoint": "obs.cn-north-4.myhuaweicloud.com",
      "Region":   "cn-north-4",
      "AK":       "${env:NCP_OBS_AK}",
      "SK":       "${env:NCP_OBS_SK}"
    }
  }
}
```

**规则:**
- URL 中引用的 profile 必须存在于已加载的配置中,否则启动期立即报错。
- `Provider` 必须等于 URL scheme(`oss://prod@...` 要求 `Profiles.prod.Provider == "oss"`; `cos://prod@...` 要求 `Profiles.prod.Provider == "cos"`; `obs://prod@...` 要求 `Profiles.prod.Provider == "obs"`)。
- `AK`/`SK`/`Endpoint`/`Region` 中的 `${env:VAR}` 占位符在加载期解析。允许写入明文凭据,但此时配置文件必须为 `0600`,否则 ncp 拒绝启动。
- **没有任何回退路径**:云 URL 缺 profile,或在 URL 中嵌入密码,直接拒绝。
- COS 的 `Endpoint` 可选。如不填写,自动构造为 `https://<bucket>.cos.<region>.myqcloud.com`。
- OBS 的 `Endpoint` 在 profile 校验阶段为必填,但运行期若缺省 SDK 会回退到 `https://obs.<region>.myhuaweicloud.com`。

**约束:**
- OSS、COS 或 OBS 参与时,`--cksum-algorithm` 必须为 `md5`(云存储后端用 Content-MD5 校验完整性,不支持 `xxh64`)。
- POSIX 元数据(mode、uid、gid、mtime、symlink target、xattr)以 `ncp-` 前缀的对象自定义元数据保存(如 `ncp-mode`、`ncp-uid`)。

示例:

```bash
# 本地 → OSS
ncp copy /data/project oss://prod@my-bucket/backup/

# OSS → 本地
ncp copy oss://prod@my-bucket/backup/ /data/restore/

# OSS → OSS(同账号)
ncp copy oss://prod@src-bucket/data/ oss://prod@dst-bucket/backup/

# OSS → OSS(跨账号)
ncp copy oss://acct-a@src-bucket/data/ oss://acct-b@dst-bucket/backup/

# 校验 OSS 数据
ncp cksum /data/project oss://prod@my-bucket/backup/

# 本地 → COS
ncp copy /data/project cos://cos-prod@my-bucket-1250000000/backup/

# COS → 本地
ncp copy cos://cos-prod@my-bucket-1250000000/backup/ /data/restore/

# COS → COS(同账号)
ncp copy cos://cos-prod@src-bucket/data/ cos://cos-prod@dst-bucket/backup/

# COS → COS(跨账号)
ncp copy cos://acct-a@src-bucket/data/ cos://acct-b@dst-bucket/backup/

# 校验 COS 数据
ncp cksum /data/project cos://cos-prod@my-bucket-1250000000/backup/

# 本地 → OBS
ncp copy /data/project obs://obs-prod@my-bucket/backup/

# OBS → 本地
ncp copy obs://obs-prod@my-bucket/backup/ /data/restore/

# OBS → OBS(同账号)
ncp copy obs://obs-prod@src-bucket/data/ obs://obs-prod@dst-bucket/backup/

# OBS → OBS(跨账号)
ncp copy obs://acct-a@src-bucket/data/ obs://acct-b@dst-bucket/backup/

# 校验 OBS 数据
ncp cksum /data/project obs://obs-prod@my-bucket/backup/
```

## 架构

```
Walker(1) ──discoverCh──→ Replicator(N) ──resultCh──→ DBWriter(1)
```

- **Walker** 遍历源端目录，将进度写入 PebbleDB，推送项目到发现通道。
- **Replicator**（N 个工作线程）从源端复制文件到目的端，流式计算校验和。
- **DBWriter** 批量处理结果并持久化到 PebbleDB。
- **背压机制**：通道满时，Walker 仅写 DB，遍历完成后回放。

进度以 2 字节值 `[CopyStatus][CksumStatus]` 存储在以相对路径为键的 DB 中，使用 `__walk_complete` 标记判断续传策略。

## 使用场景

### 场景一：先复制，再校验（确保复制结果正确）

适用于已有一份复制结果，需要验证数据一致性的情况。

```bash
# 1. 复制文件
ncp copy /data/project /backup/project
# 输出中包含 taskId，例如 task-20260502-143000-abcd

# 2. 校验已复制的文件
ncp cksum --task task-20260502-143000-abcd

# 3. 如果校验发现不一致，只重新复制校验失败的文件
ncp copy --task task-20260502-143000-abcd

# 也可以用 resume 自动识别上次的任务类型
ncp resume task-20260502-143000-abcd
```

步骤 3 仅重新复制 `cksumStatus != pass` 的文件，`ResumeFromDB` 会跳过 `CksumPass` 或 `CopyDone+CksumNone` 的文件。

### 场景二：先校验，再增量复制（基于已有数据做增量）

适用于目的端已有部分数据，先用校验找出差异，再只复制不一致的文件。

```bash
# 1. 校验源端和目的端的差异
ncp cksum /data/project /backup/project
# 输出中包含 taskId，例如 task-20260502-150000-ef01

# 2. 基于校验结果，只复制不一致的文件
ncp copy --task task-20260502-150000-ef01
# 此时 copy 会跳过 cksumStatus=pass 的文件，只复制 mismatch/error/none 的文件
```

这种模式适合增量同步场景：先用 cksum 快速定位差异文件，再用 copy 精准补充，避免全量重复复制。

## FileLog

FileLog 是 ncp 的结构化事件流 —— 每个文件操作都会输出一行 JSON，方便 Agent 和脚本实时追踪进度。

### 事件格式

所有事件均为 NDJSON 格式（每行一个 JSON 对象）。每个事件包含：

```json
{"timestamp": "2026-05-03T14:30:00.123456789Z", "event": "<类型>", "taskId": "task-20260502-143000-abcd", ...}
```

### 事件类型

#### `file_complete` — 文件内容级事件，每个普通文件批量刷新后触发

报告普通文件的**内容复制**结果（写入数据 + 关闭文件）。每个文件只产生一条。

复制模式：

```json
{
  "timestamp": "2026-05-03T14:30:01.234567890Z",
  "event": "file_complete",
  "taskId": "task-20260502-143000-abcd",
  "action": "copy",
  "result": "done",
  "errorCode": "",
  "relPath": "src/main.go",
  "fileType": "regular",
  "fileSize": 4096,
  "algorithm": "md5",
  "checksum": "d41d8cd98f00b204e9800998ecf8427e"
}
```

- `result`：`"done"`、`"error"` 或 `"skipped"`。`"skipped"` 表示文件被 mtime/size/ETag 匹配跳过。
- `checksum`：流式校验和的十六进制字符串（仅内容复制）。

校验模式：

```json
{
  "timestamp": "2026-05-03T14:35:00.345678901Z",
  "event": "file_complete",
  "taskId": "task-20260502-150000-ef01",
  "action": "cksum",
  "result": "done",
  "errorCode": "",
  "relPath": "src/main.go",
  "fileType": "regular",
  "fileSize": 4096,
  "algorithm": "md5",
  "checksum": "",
  "srcHash": "d41d8cd98f00b204e9800998ecf8427e",
  "dstHash": "d41d8cd98f00b204e9800998ecf8427e"
}
```

- `result`：`"done"`（通过）或 `"error"`（不一致/错误）。`srcHash` 和 `dstHash` 仅对普通文件填充。

#### `file_metadata_complete` — 文件元数据事件，每个文件批量刷新后触发

报告**元数据操作**结果（Mkdir / Symlink / SetMetadata）。所有文件类型均产生此事件，每个文件只产生一条。

```json
{
  "timestamp": "2026-05-03T14:30:01.234567890Z",
  "event": "file_metadata_complete",
  "taskId": "task-20260502-143000-abcd",
  "result": "done",
  "errorCode": "",
  "relPath": "src/main.go",
  "fileType": "regular"
}
```

- 目录和符号链接**只**产生此事件（不产生 `file_complete`）。
- 普通文件同时产生 `file_complete`（内容）和 `file_metadata_complete`（元数据）。
- `result`：`"done"` 或 `"error"`。

#### `progress_summary` — 周期性触发（由 `--FileLogInterval` 控制）

```json
{
  "timestamp": "2026-05-03T14:30:05.456789012Z",
  "event": "progress_summary",
  "taskId": "task-20260502-143000-abcd",
  "phase": "copy",
  "finished": false,
  "exitCode": 0,
  "walker": {
    "walkComplete": true,
    "discoveredCount": 1000000,
    "dispatchedCount": 500000,
    "backlogCount": 500000,
    "channelFull": false
  },
  "replicator": {
    "filesCopied": 480000,
    "bytesCopied": 107374182400,
    "filesPerSec": 3200.5,
    "bytesPerSec": 715827882.7
  },
  "dbWriter": {
    "pendingCount": 50,
    "totalDone": 480000,
    "totalFailed": 3,
    "totalProcessed": 480003
  }
}
```

- `phase`：`"copy"` 或 `"cksum"`。
- `finished`：任务完成时为 `true`。
- `exitCode`：仅在 `finished=true` 时有意义。`0` = 全部通过，`2` = 有错误/不一致。

### 使用 FileLog

**给 Agent** — 实时监听 FileLog 并响应事件：

```bash
# 实时监控不一致文件
tail -f /tmp/ncp_file_log.json | jq 'select(.event=="file_complete" and .result=="error")'

# 跟踪进度
tail -f /tmp/ncp_file_log.json | jq 'select(.event=="progress_summary") | {phase, filesCopied: .replicator.filesCopied, bytesPerSec: .replicator.bytesPerSec}'

# 检测任务完成
tail -f /tmp/ncp_file_log.json | jq 'select(.event=="progress_summary" and .finished==true)'
```

**给用户** — 通过 `jq` 获取可读输出：

```bash
# 查看每个已完成的文件
cat /tmp/ncp_file_log.json | jq -c '{event, relPath: .relPath, result: .result}'

# 仅显示错误
cat /tmp/ncp_file_log.json | jq 'select(.result=="error")'
```

**配置项**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--enable-FileLog` | true | 启用/禁用 FileLog |
| `--FileLogOutput` | /tmp/ncp_file_log.json | 输出目标：`console` 输出到标准输出，或指定文件路径 |
| `--FileLogInterval` | 5 | `progress_summary` 事件输出间隔（秒） |

## 开发

```bash
make build          # 构建二进制
make test           # 运行所有测试
make unit           # 仅单元测试
make integration    # 仅集成测试
make lint           # 运行 golangci-lint
```

## 许可证

GNU General Public License v3.0
