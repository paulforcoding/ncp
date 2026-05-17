# 跳过策略

## 触发条件

当以下情况出现时读这个文件：
- 不确定 ncp 何时会跳过文件
- FileLog 中出现 `result: "skipped"` 但不理解为什么
- 需要知道 `--skip-by-mtime` 和 `--no-skip-by-mtime` 的区别
- 想了解 OSS ETag 跳过机制

## 快速行动

ncp 默认开启 `--skip-by-mtime`，会跳过目标端已存在的相同文件。用 `--no-skip-by-mtime` 强制复制/校验所有文件。

## 详情

### 跳过判断逻辑

当 `--skip-by-mtime` 开启时，Replicator/CksumWorker 对每个文件调用 `Stat` 检查目标端：

**目录：** 目标端存在同名目录 → 直接跳过（不比较任何属性）

**符号链接：** 目标端存在同名符号链接且 target 相同 → 跳过

**普通文件：** 按以下优先级判断：

1. 如果两端都有 checksum 且算法相同 → 比较 checksum，相同则跳过
2. 如果两端都有 mtime → 比较 mtime + size，都相同则跳过
3. 否则不跳过

### 各存储后端的跳过行为

| 后端 | 跳过依据 | 说明 |
|------|---------|------|
| 本地 → 本地 | mtime + size | 文件的 mtime 和 size 都相同则跳过 |
| 本地 → ncp:// | mtime + size | 远端 Stat 返回的 mtime + size |
| 本地 → OSS | mtime + size | OSS 的 Last-Modified 映射为 mtime |
| ncp:// → 本地 | mtime + size | 远端 Stat 返回的属性 |
| OSS → OSS | ETag (md5) | OSS 单分片上传的 ETag 就是 Content-MD5 |

### skip-by-mtime 对 copy 和 cksum 的区别

| 命令 | skip-by-mtime=true 时的行为 |
|------|---------------------------|
| `ncp copy` | 跳过 mtime+size 相同的文件，标记为 `skipped`，CopyStatus=done |
| `ncp cksum` | 跳过 mtime+size 相同的文件，标记为 `skipped`，CksumStatus=pass |

**注意：** cksum 中 skip 不意味着"没校验"，而是"根据 mtime+size 判断两端一致，视为 pass"。如果需要严格逐字节校验，用 `--no-skip-by-mtime`。

### FileLog 中的 skipped 事件

```json
{
  "event": "file_complete",
  "action": "copy",
  "result": "skipped",
  "relPath": "a/b.txt",
  "fileSize": 1024
}
```

- `result: "skipped"` 表示文件被跳过，未实际复制
- skipped 文件不计入 `replicator.bytesCopied`（因为没传数据）
- skipped 文件计入 `dbWriter.totalDone`（视为已完成）

### 常见场景

**增量同步：** `--skip-by-mtime`（默认）只复制新增/修改的文件，上次已复制的文件被跳过

**全量复制：** `--no-skip-by-mtime` 强制复制所有文件，即使目标端已存在

**先 cksum 后 copy：** cksum 发现 mismatch 的文件，后续 `ncp copy --task <id>` 会重传（因为 CopyStatus=error 的文件不会被跳过）

### 常见陷阱：copy 后 cksum 一秒完成

copy 正常结束后，如果立即执行 `ncp cksum --task <taskID>`（默认 `--skip-by-mtime`），cksum 会发现所有文件的 mtime+size 都匹配，**瞬间全部跳过**，看起来"校验通过了"但实际上没有逐字节比对。

要真正校验数据完整性，必须加 `--no-skip-by-mtime`：

```bash
ncp cksum --task <taskID> --no-skip-by-mtime
```

这样 cksum 会逐文件读取两端内容并计算 checksum 比对，速度较慢但结果可靠。

## 关联文件

- progress-model.md — CopyStatus/CksumStatus 状态机
- copy.md — copy 命令的 skip-by-mtime flag
- cksum.md — cksum 命令的 skip-by-mtime flag
