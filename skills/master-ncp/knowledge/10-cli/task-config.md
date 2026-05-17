# ncp task / config

## 触发条件

当以下情况出现时读这个文件：
- 需要查看、管理 ncp 任务
- 需要查看生效配置或 profile
- 需要获取 taskID 以便 resume 或 cksum
- 不确定 task 子命令的用法

## 快速行动

```bash
ncp task list                        # 列出所有任务（每行一个 JSON）
ncp task show <taskID>               # 查看任务详情（缩进 JSON）
ncp task delete <taskID>             # 删除任务
ncp config show                      # 查看生效配置（AK/SK 脱敏）
ncp config show --profile <name>     # 查看指定 profile
```

## 详情

### task list

输出为 JSON Lines，每行一个任务元数据：

```json
{"taskId":"task-xxx","srcBase":"/data","dstBase":"/backup","createdAt":"...","cmdArgs":["copy","/data","/backup"],"pid":12345,"hostname":"myhost","runs":[{"id":"run-xxx","jobType":"copy","startedAt":"...","finishedAt":"...","status":"done"}]}
```

### task show

输出缩进 JSON，包含任务的所有 run 记录：

```json
{
  "taskId": "task-xxx",
  "srcBase": "/data",
  "dstBase": "/backup",
  "runs": [
    {"id": "run-1", "jobType": "copy", "status": "done"},
    {"id": "run-2", "jobType": "cksum", "status": "failed", "exitCode": 2}
  ]
}
```

### task delete

输出确认 JSON：`{"taskId":"task-xxx","action":"deleted"}`

**注意：** 如果任务正在运行，delete 会被拒绝。先停止 ncp 进程再删除。

### config show

显示所有生效配置值，AK/SK 自动脱敏。含配置来源说明（默认值/环境变量/配置文件/flag）。

用 `--profile <name>` 只看指定 profile 的配置。

### dry-run

任何执行命令都可以加 `--dry-run` 预览生效配置而不实际执行：

```bash
ncp copy /data /backup --dry-run
ncp cksum /data /backup --dry-run
ncp resume task-xxx --dry-run
```

### 公共 flag

所有 task 子命令共用：

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--ProgressStorePath` | /tmp/ncp_progress_store | 任务元数据存储目录 |

## 关联文件

- resume.md — 用 taskID 恢复中断任务
- profiles.md — 云存储凭据配置
