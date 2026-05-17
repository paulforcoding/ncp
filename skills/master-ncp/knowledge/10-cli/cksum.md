# ncp cksum

## 触发条件

当以下情况出现时读这个文件：
- 需要校验源和目标的数据一致性
- 不确定 cksum 的参数和约束
- 需要理解 cksum 与 copy 的路径语义差异

## 快速行动

**首次运行**（新建 task，两端为显式基址）：
```bash
ncp cksum <src> <dst> \
  --FileLogOutput /tmp/ncp_file.log --FileLogInterval 10 \
  --ProgramLogOutput /tmp/ncp_program.log --ProgramLogLevel warn
```

**后续运行**（基于已有 task，如 copy 完成后校验完整性）：
```bash
ncp cksum --task <taskID> \
  --FileLogOutput /tmp/ncp_file.log --FileLogInterval 10 \
  --ProgramLogOutput /tmp/ncp_program.log --ProgramLogLevel warn
```

两种模式的区别：首次运行必须提供 `<src> <dst>`，`--task` 模式从 meta.json 读取 src/dst，不需要再传路径。执行后立即监控 FileLog，记录 taskId。

## 详情

### 路径语义（与 copy 不同）

cksum 的 src 和 dst 是**显式基址**，不做 basename 自动拼接：

```bash
# copy: dst 存在时自动加 basename 前缀
ncp copy /data/dir /tmp/dir    → 比对 /data/dir/* vs /tmp/dir/dir/*

# cksum: 两端直接比对，无自动 join
ncp cksum /data/dir /tmp/dir   → 比对 /data/dir/* vs /tmp/dir/*
```

### cksum 不支持 ncp:// 目标

`ncp cksum <src> ncp://...` 不支持——ncp:// 协议层已有内置 MD5 校验。如果需要校验 ncp:// 目标的数据，使用 `--task` 模式：

```bash
ncp cksum --task <taskID>
```

### 完整 flags

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--CopyParallelism` | 2 | 并行校验 worker 数量 |
| `--FileLogOutput` | console | FileLog 输出 |
| `--FileLogInterval` | 5 | progress_summary 间隔（秒） |
| `--ProgramLogOutput` | console | ProgramLog 输出 |
| `--ProgramLogLevel` | info | 日志级别 |
| `--ProgressStorePath` | /tmp/ncp_progress_store | 进度存储目录 |
| `--cksum-algorithm` | md5 | 校验算法：md5 或 xxh64 |
| `--skip-by-mtime` | true | 跳过 mtime+size 一致的文件 |
| `--no-skip-by-mtime` | false | 校验所有文件 |
| `--enable-FileLog` | true | 启用 FileLog |
| `--disable-FileLog` | false | 禁用 FileLog |
| `--dry-run` | false | 预览配置后退出 |
| `--task` | "" | 指定 taskID 校验已有任务（后续运行模式，不需要 `<src> <dst>`） |

### cksum-algorithm 约束

- **OSS/COS/OBS 必须用 md5**（云存储用 Content-MD5 校验）
- 本地和 ncp:// 场景可选 `xxh64` 提速

### FileLog 中的 cksum 事件

cksum 模式下 `file_complete` 事件额外包含 `srcHash` 和 `dstHash`：

```json
{
  "event": "file_complete",
  "action": "cksum",
  "result": "mismatch",
  "srcHash": "abc123",
  "dstHash": "def456"
}
```

- `result`: `done`（pass）/ `mismatch`（不一致）/ `error`（失败）/ `skipped`（跳过）

### 典型工作流

**先复制，后校验：**
```bash
ncp copy /data/project /backup/project          # 生成 task-xxx
ncp cksum --task task-xxx --no-skip-by-mtime     # 逐字节校验（必须加 --no-skip-by-mtime，否则 mtime+size 匹配会全部跳过）
ncp copy --task task-xxx                         # 仅重传校验失败的文件
```

**先校验，后增量复制：**
```bash
ncp cksum /data/dir /backup/dir                  # 比对差异，生成 task-xxx
ncp copy --task task-xxx                          # 基于 task 仅复制 mismatch 的文件
```

## 关联文件

- copy.md — 复制命令
- resume.md — 恢复中断的校验任务
- serve.md — cksum --task 目标为 ncp:// 时需重启 serve
- log.md — 三条信息通道与监控方案
