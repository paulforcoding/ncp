# 路径语义

## 触发条件

当以下情况出现时读这个文件：
- 不确定 ncp copy 的 dst 路径会如何处理
- 不理解 copy 和 cksum 的路径语义差异
- 用户问"复制后的目录结构是什么样的"
- 遇到 ncp:// 目标路径与预期不符

## 快速行动

**copy 路径语义对齐 cp：** dst 是否存在决定是否加 basename 前缀。**cksum 不加前缀，两端直接比对。**

## 详情

### copy 路径语义

| 条件 | 行为 | 示例 |
|------|------|------|
| 单源 + dst 不存在 | 复制 AS dst（不加 basename 前缀） | `ncp copy /data/dir /tmp/newname` → `/tmp/newname/...` |
| 单源 + dst 已存在且为目录 | 复制 INTO dst（加 basename 前缀） | `ncp copy /data/dir /tmp/existing` → `/tmp/existing/dir/...` |
| 多源 + dst 已存在且为目录 | 复制 INTO dst（每个源加 basename 前缀） | `ncp copy a b /tmp/existing` → `/tmp/existing/a/...`, `/tmp/existing/b/...` |
| 多源 + dst 不存在 | **报错** | — |

**判断逻辑：** ncp 在执行前检查 dst 是否存在且为目录（`ExistsDir`）。是 → 加前缀；否 → 不加。

### ncp:// 目标的特殊行为

ncp:// 目标无法预先检查目录是否存在（需要连接才能判断），因此**始终视为不存在**，即不加 basename 前缀：

```bash
ncp copy /data/dir ncp://host:9900/root/
# 结果：/root/...（不是 /root/dir/...）
```

如果需要加 basename 前缀，把 basename 写进 URL 路径：

```bash
ncp copy /data/dir ncp://host:9900/root/dir/
```

### 云存储目标

OSS/COS/OBS 目标可以检查 bucket+prefix 是否存在（通过 `ExistsDir`），因此行为与本地一致。

### cksum 路径语义

cksum 的 src 和 dst 是**显式基址**，不做 basename 自动拼接：

```bash
# copy: dst 存在时加 basename 前缀
ncp copy /data/dir /tmp/dir    → 结果在 /tmp/dir/...

# cksum: 两端直接比对
ncp cksum /data/dir /tmp/dir   → 比对 /data/dir/* vs /tmp/dir/*
```

**cksum 的 src 和 dst 必须指向同一层级的内容**，如果 copy 时加了 basename 前缀，cksum 时 dst 也要包含那个 basename：

```bash
ncp copy /data/dir /tmp/existing    → 结果在 /tmp/existing/dir/...
ncp cksum /data/dir /tmp/existing/dir  # 正确：两端对齐
ncp cksum /data/dir /tmp/existing      # 错误：比对层级不匹配
```

或者用 `--task` 模式避免路径对齐问题：

```bash
ncp cksum --task <taskID>    # 自动使用 task 记录的 src/dst
```

## 关联文件

- copy.md — copy 命令
- cksum.md — cksum 命令
