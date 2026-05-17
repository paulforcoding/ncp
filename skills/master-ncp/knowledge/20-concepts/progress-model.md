# 进度状态机

## 触发条件

当以下情况出现时读这个文件：
- 需要理解 ncp 如何跟踪每个文件的复制/校验状态
- 不理解 resume 时 DB replay 的行为
- 需要理解 FileLog 中 file_complete.result 的含义

## 快速行动

每个文件在 PebbleDB 中存储 2 字节：`[CopyStatus][CksumStatus]`。ncp 通过这 2 字节判断文件是否完成、是否需要重试，resume 时据此决定跳过哪些文件。

## 详情

### CopyStatus（字节 1）

```
discovered(1) → dispatched(2) → done(3) / error(4)
```

| 值 | 状态 | 含义 |
|----|------|------|
| 1 | discovered | Walker 已发现，未推送到 channel |
| 2 | dispatched | 已推送到 discoverCh，等待 Replicator 处理 |
| 3 | done | 复制成功 |
| 4 | error | 复制失败 |

### CksumStatus（字节 2）

```
none(0) → pending(1) → pass(2) / mismatch(3) / error(4)
```

| 值 | 状态 | 含义 |
|----|------|------|
| 0 | none | 未校验 |
| 1 | pending | 等待校验 |
| 2 | pass | 校验通过 |
| 3 | mismatch | 校验不匹配 |
| 4 | error | 校验出错 |

### 与 FileLog 的对应

| FileLog result | 含义 | 对应 DB 状态 |
|---------------|------|-------------|
| `done` | 复制/校验成功 | CopyStatus=3 或 CksumStatus=2 |
| `skipped` | mtime+size 一致被跳过 | 不更新（保持上次状态） |
| `error` | 失败 | CopyStatus=4 或 CksumStatus=4 |
| `mismatch` | cksum 不一致 | CksumStatus=3 |

### __walk_complete 标记

Walker 完成遍历后写入特殊键 `__walk_complete`，记录文件总数。这个标记决定 resume 行为：

- 存在 → resume 时直接从 DB replay，跳过已完成的文件
- 不存在 → 清空 DB，重新遍历（代价极高）

**这就是 `progress_summary` 中 `walkComplete=true` 必须主动报告的原因**——它标志着 resume 可以跳过遍历。

### copy + cksum 组合工作流

DB 中每个文件同时维护 CopyStatus 和 CksumStatus，可以独立演进：

```bash
# 先复制（CopyStatus: discovered → done）
ncp copy /data/project /backup/project

# 后校验（CksumStatus: none → pass/mismatch）
ncp cksum --task task-xxx

# 重传校验失败的（CopyStatus: error → done，仅处理 CksumStatus=mismatch 的文件）
ncp copy --task task-xxx
```

## 关联文件

- log.md — progress_summary 中 walker/replicator/dbWriter 字段含义
- skip-logic.md — 跳过策略
- resume.md — resume 如何利用 DB 状态
