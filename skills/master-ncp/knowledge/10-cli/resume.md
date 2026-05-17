# ncp resume

## 触发条件

当以下情况出现时读这个文件：
- 复制或校验任务中断后需要恢复
- 不确定 resume 的行为（是重新遍历还是从 DB replay）
- 用户说"继续复制"或"恢复任务"

## 快速行动

```bash
ncp resume <taskID> \
  --FileLogOutput /tmp/ncp_file.log --FileLogInterval 10 \
  --ProgramLogOutput /tmp/ncp_program.log --ProgramLogLevel warn
```

执行后立即监控 FileLog。如果目标为 ncp://，先重启远端 serve。

## 详情

### resume 行为

resume 自动检测上次 run 的 jobType（copy 或 cksum），按相同模式继续：

| 遍历状态 | resume 行为 |
|----------|------------|
| `__walk_complete` 存在 | 从 DB replay，跳过已完成的文件，不重新遍历 |
| `__walk_complete` 不存在 | 清空 DB，重新遍历（代价高） |

**`__walk_complete` 是 Walker 完成后写入 DB 的标记。** 这就是为什么 `walkComplete=true` 是关键事件——它决定了 resume 是否需要重新遍历。

### 完整 flags

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--CopyParallelism` | 2 | 并行 worker 数量 |
| `--FileLogOutput` | console | FileLog 输出 |
| `--FileLogInterval` | 5 | progress_summary 间隔（秒） |
| `--ProgramLogOutput` | console | ProgramLog 输出 |
| `--ProgramLogLevel` | info | 日志级别 |
| `--ProgressStorePath` | /tmp/ncp_progress_store | 进度存储目录 |
| `--skip-by-mtime` | true | 跳过 mtime+size 一致的文件 |
| `--no-skip-by-mtime` | false | 不跳过，处理所有文件 |
| `--enable-FileLog` | true | 启用 FileLog |
| `--disable-FileLog` | false | 禁用 FileLog |
| `--dry-run` | false | 预览配置后退出 |

### 如何获取 taskID

```bash
# 查看所有任务
ncp task list

# 查看最新任务
ls -t /tmp/ncp_progress_store | head -1
```

### 目标为 ncp:// 时

resume 前必须先重启远端 serve——因为 serve 在上次 task 完成后已退出：

```bash
ssh <host> "nohup ncp serve --listen :9900 </dev/null >>/tmp/ncp-serve.log 2>&1 &"
```

### 可以修改的参数

resume 时可以调整 `--CopyParallelism`、`--ChannelBuf`（通过 ncp copy --task）等参数。这在卡住后降级恢复时有用。

## 关联文件

- copy.md — 复制命令
- cksum.md — 校验命令
- serve.md — 目标为 ncp:// 时需重启 serve
- log.md — 三条信息通道与监控方案
