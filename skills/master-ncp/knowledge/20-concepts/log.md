# ncp 三条信息通道

## 触发条件

当以下情况出现时读这个文件：
- 需要监控 ncp 的复制/校验进度
- 需要解析 FileLog 中的 NDJSON 事件
- 不确定 stderr/stdout、ProgramLog、FileLog 三条通道的区别和用途
- 需要知道如何向人类报告 ncp 的运行状态

## 快速行动

ncp 有三条信息通道，Agent 必须同时关注。执行 ncp 后，用 Monitor 工具 tail FileLog 过滤 `progress_summary` 事件；如果 ncp 启动失败或异常退出，先看 stderr，再看 ProgramLog。

```bash
# 执行 ncp 命令（确保注入了 FileLog 和 ProgramLog 参数）
ncp copy <src> <dst> \
  --FileLogOutput /tmp/ncp_file.log --FileLogInterval 10 \
  --ProgramLogOutput /tmp/ncp_program.log --ProgramLogLevel warn

# 启动后立即监控 FileLog
tail -f /tmp/ncp_file.log | grep --line-buffered '"progress_summary"'
```

每收到一个 `progress_summary`，立即向用户报告人性化进度。`finished=true` 时任务完成。

## 详情

### 通道 1：stderr / stdout

ncp 只在以下情况输出到 stderr/stdout，**正常运行期间不输出任何内容**：

| 时机 | 输出到 | 内容 | 示例 |
|------|--------|------|------|
| 参数错误 | stderr | Cobra 框架的错误信息 | `Error: unknown flag "--base"` |
| `--help` | stdout | 帮助文本 | 命令用法、可用 flag 列表 |
| `ncp task list` | stdout | 每行一个 JSON，列出所有任务 | `{"taskId":"task-xxx","srcBase":"/data",...}` |
| `ncp task show <id>` | stdout | 缩进 JSON，单个任务详情 | `{"taskId":"task-xxx","runs":[...]}` |
| `ncp task delete <id>` | stdout | 确认删除的 JSON | `{"taskId":"task-xxx","action":"deleted"}` |
| `ncp config show` | stdout | 生效配置 | 含所有 flag 值，AK/SK 脱敏 |

**Agent 行为：**
- 执行 ncp 命令时，始终捕获 stderr/stdout（通过 `2>&1`）
- 如果 ncp 立即退出（exit code 1），先检查 stderr 中的错误信息
- 查询命令（task list/show/delete、config show）的结果直接从 stdout 读取

### 通道 2：ProgramLog

ncp 的内部诊断日志，通过 `--ProgramLogOutput` 和 `--ProgramLogLevel` 控制。输出为 JSON 格式，每行一条。

**格式：**
```json
{"time":"2026-05-17T11:50:45.667777+08:00","level":"ERROR","msg":"fatal error","error":"..."}
{"time":"2026-05-17T11:50:45.6882+08:00","level":"ERROR","msg":"copy job failed","taskId":"task-xxx","error":"...","exitCode":2}
```

**级别：** `ProgramLogLevel` 控制最低输出级别，从低到高：trace / debug / info / warn / error / critical

**各级别的典型输出：**

| 级别 | 何时输出 | 示例 msg |
|------|---------|----------|
| INFO | serve 启动 | `"ncp serve started", "listen", ":9900"` |
| WARN | 非致命异常，任务可继续 | `"ensure dir mtime failed"`, `"server cleanup failed"` |
| ERROR | 致命错误，任务终止 | `"fatal error"`, `"copy job failed"`, `"cksum job failed"`, `"resume failed"` |

**`--ProgramLogLevel warn`（推荐）：** 正常运行时 ProgramLog 无输出，只有出现 warn/error 时才写日志——既减少噪音，又不遗漏问题。

**Agent 行为：**
- ncp 进程异常退出时，立即 `cat /tmp/ncp_program.log` 查找根因
- ProgramLog 中的 `error` 字段包含最具体的诊断信息（如 `connection refused`、`dial tcp`）

### 通道 3：FileLog

ncp 面向外部的结构化输出，通过 `--FileLogOutput` 和 `--FileLogInterval` 控制。输出为 NDJSON 格式，每行一个 JSON 对象。

**三种事件：**

| 事件 | 含义 | 频率 |
|------|------|------|
| `file_complete` | 单个文件内容复制/校验完成 | 每个普通文件一条 |
| `file_metadata_complete` | 单个文件元数据操作完成 | 每个文件一条（含目录和符号链接） |
| `progress_summary` | 整个作业的进度、性能、计数器 | 定期（每 FileLogInterval 秒），完成时发出最终汇总 |

#### file_complete

```json
{
  "event": "file_complete",
  "taskId": "task-xxx",
  "action": "copy",
  "result": "done",
  "relPath": "a/b.txt",
  "fileType": "regular",
  "fileSize": 1024,
  "algorithm": "md5",
  "checksum": "d41d8cd98f00b204e9800998ecf8427e"
}
```

- `result`: `done`（成功）/ `error`（失败）/ `skipped`（被跳过）
- `skipped`: mtime+size 一致时跳过（或 OSS 的 ETag 一致）
- cksum 模式额外包含 `srcHash` 和 `dstHash`

#### file_metadata_complete

```json
{
  "event": "file_metadata_complete",
  "taskId": "task-xxx",
  "result": "done",
  "relPath": "a/b.txt",
  "fileType": "regular"
}
```

- `fileType`: `regular` / `dir` / `symlink`
- 目录和符号链接只产生此事件，不产生 `file_complete`

#### progress_summary

```json
{
  "event": "progress_summary",
  "taskId": "task-xxx",
  "phase": "copy",
  "finished": false,
  "exitCode": 0,
  "walker": {
    "walkComplete": true,
    "discoveredCount": 1000000,
    "dispatchedCount": 500000,
    "backlogCount": 0,
    "channelFull": false
  },
  "replicator": {
    "filesCopied": 480000,
    "bytesCopied": 107374182400,
    "filesPerSec": 3200.5,
    "bytesPerSec": 715827882.7
  },
  "dbWriter": {
    "totalDone": 480000,
    "totalFailed": 3,
    "totalProcessed": 480003,
    "pendingCount": 0
  }
}
```

- `finished=true` 时为最终汇总，`exitCode` 0 = 成功，2 = 有错误/不匹配
- `walker.discoveredCount` = 文件总数（遍历完成后此值为总数）
- `replicator.filesCopied` / `bytesCopied` = 已复制的文件数/字节数
- `dbWriter.totalFailed` > 0 表示有失败文件

### 监控方案

**执行阶段（强制性）：**

1. 确保命令包含 FileLog 和 ProgramLog 参数（缺失时 Agent 自动注入）
2. 执行 ncp 后立即启动 Monitor 跟踪 FileLog：

```bash
tail -f /tmp/ncp_file.log | grep --line-buffered '"progress_summary"'
```

3. 每收到 `progress_summary` 向用户报告：

```
进度：480,000 / 1,000,000 文件 (48.0%)
  已复制：   480,000 文件，100.0 GB
  速度：     3,201 文件/秒，715.8 MB/秒
  失败：     3 文件
  遍历：     完成（已发现 1,000,000）
```

**数字格式化规则：**
- 文件数：逗号分隔（`1,000,000`）
- 字节数：转为 KB/MB/GB/TB
- 百分比：保留一位小数
- 速度：根据数量级选合适单位

**完成时：** `finished=true` → 停止 Monitor，进入结果报告阶段。

**错误时：**
1. ncp 立即退出（exit code 1）→ 看 stderr/stdout 中的参数错误
2. ncp 运行中异常终止 → 看 ProgramLog（`cat /tmp/ncp_program.log`），提取 connection refused、fatal error 等根因
3. ncp 正常结束但有失败 → 看 FileLog 的 `progress_summary`（exitCode=2 或 totalFailed > 0）
4. 向用户报告错误 + 建议后续操作（resume、cksum）

**健康检查（建议每 60 秒）：**
- 磁盘空间：`df -h` 检查 FileLog/ProgressStore 所在文件系统
- 卡住检测：连续 3 个 `progress_summary` 中 filesCopied + bytesCopied + totalProcessed 均不变
- 内存：`ps -o rss= -p <pid>` 检查 ncp 进程 RSS

## 关联文件

- progress-model.md — 进度状态机详解（CopyStatus/CksumStatus）
- skip-logic.md — 跳过策略（何时 result=skipped）
- exit-codes.md — 退出码含义
