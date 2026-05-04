# 步骤 3：报告结果

输出最终结果并提供后续操作建议。

## 3.1 解析最终结果

从 FileLog 读取最后的 `progress_summary` 事件（`finished=true` 的那条）：

```bash
grep '"progress_summary"' /tmp/ncp_file_log.json | tail -1 | jq '.'
```

提取关键指标：
- `exitCode` — 0（成功）或 2（有错误）
- `replicator.filesCopied` — 已复制文件总数
- `replicator.bytesCopied` — 已复制字节总数
- `dbWriter.totalDone` — 成功完成的文件数
- `dbWriter.totalFailed` — 失败的文件数
- `dbWriter.totalProcessed` — 已处理的文件总数

同时提取 taskID：

```bash
ls -t /tmp/ncp_progress_store | head -1
```

## 3.2 展示报告

为用户格式化最终报告：

**成功时（exitCode 0）：**

```
复制成功完成！

  任务 ID：    task-20260503-143000-abcd
  已复制文件： 1,000,000
  已复制数据： 500.0 GB
  耗时：       约 2小时15分

数据已复制到：/backup/project
```

**部分失败时（exitCode 2）：**

```
复制完成，但有错误。

  任务 ID：      task-20260503-143000-abcd
  成功文件：     999,997
  失败文件：     3
  已复制数据：   500.0 GB

失败文件：
  - src/corrupt_file.bin：权限被拒绝
  - src/locked_file.dat：文件被占用
  - src/broken_link：断开的符号链接

后续操作：
  1. 修复问题后重试：ncp resume task-20260503-143000-abcd
  2. 验证已成功文件：ncp cksum --task task-20260503-143000-abcd
```

**查看失败文件详情**，搜索 FileLog：

```bash
grep '"file_complete"' /tmp/ncp_file_log.json | jq 'select(.result=="error")' 
```

## 3.3 提供后续操作建议

询问用户是否需要：

1. **验证数据一致性** — 运行 `ncp cksum` 确认所有已复制文件与源一致
2. **恢复失败文件** — 如果有错误，运行 `ncp resume` 重试
3. **清理** — 删除 FileLog 和进度存储文件

以具体命令的形式展示，或主动提出代为执行。

## 3.4 清理（可选）

用户同意后，清理临时文件：

```bash
rm -f /tmp/ncp_file_log.json
```

**不要删除进度存储**（`/tmp/ncp_progress_store`）— resume 和 cksum 操作需要它。仅在用户明确要求并理解后果时才删除。

## 3.5 结束

工作流完成。保持可用，等待后续问题或新的 ncp 操作。
