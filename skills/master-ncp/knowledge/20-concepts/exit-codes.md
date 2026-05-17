# 退出码

## 触发条件

当以下情况出现时读这个文件：
- ncp 命令退出后需要判断成功还是失败
- 不确定 exit code 0 和 2 的区别
- 需要决定是否可以安全 resume

## 快速行动

| 退出码 | 含义 | 后续操作 |
|--------|------|---------|
| 0 | 所有文件复制/校验成功 | 无需操作 |
| 1 | 参数错误或启动失败 | 检查 stderr 中的错误信息，修正命令 |
| 2 | 部分文件失败/不匹配，或 fatal error | 可 `ncp resume <taskID>` 重试 |

## 详情

### 退出码来源

ncp 进程的退出码由 Job 的 `finalize` 函数决定：

**copy 任务：**

| 条件 | exitCode |
|------|----------|
| 无 fatal error、无 walk 错误、无失败文件 | 0 |
| fatal error（如 connection refused） | 2 |
| walk 失败 | 2 |
| 有失败文件（totalFailed > 0） | 2 |

**cksum 任务：**

| 条件 | exitCode |
|------|----------|
| 无 fatal error、无 walk 错误、无 mismatch、无失败 | 0 |
| fatal error | 2 |
| walk 失败 | 2 |
| 有 mismatch 或 failed 文件 | 2 |

### Cobra 框架的 exit code 1

参数错误、flag 缺失等由 Cobra 框架处理，输出到 stderr 并 `os.Exit(1)`。这发生在 ncp 业务逻辑之前，不产生 FileLog 或 ProgramLog。

### progress_summary 中的 exitCode

FileLog 的 `progress_summary` 事件也包含 `exitCode` 字段。`finished=true` 时的 `exitCode` 就是进程的退出码。

### exitCode 2 不是完全失败

**exitCode 2 意味着"部分文件有问题"，不是"全部失败"。** 大部分文件可能已成功复制/校验。处理方式：

1. 从 FileLog 获取失败文件列表：
   ```bash
   grep '"file_complete"' /tmp/ncp_file.log | grep '"result":"error"'
   ```
2. resume 重试失败文件：
   ```bash
   ncp resume <taskID>
   ```
3. 或针对具体问题修复后重试

### 与 Agent 的关系

Agent 执行 ncp 后应根据退出码决定下一步：
- 0 → 向用户报告成功
- 1 → 检查 stderr，向用户报告参数错误
- 2 → 检查 ProgramLog + FileLog，向用户报告部分失败，建议 resume 或 cksum

## 关联文件

- log.md — 三条信息通道
- resume.md — 恢复中断/失败任务
