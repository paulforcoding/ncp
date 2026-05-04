# 步骤 1：收集需求

收集构建 ncp 命令所需的所有信息。使用 AskUserQuestion 进行结构化选择，并根据需要追问澄清。

## 1.1 确定源路径

向用户询问源路径。ncp 支持多个源（仅限本地路径）。

- **本地路径**：如 `/data/project`、`/home/user/docs`
- **OSS 路径**：如 `oss://my-bucket/data/`（需要 OSS 凭据）

如果用户提供本地路径，先验证路径是否存在。

## 1.2 确定目标

向用户询问目标。协议决定后端类型：

| 目标类型 | 示例 | 额外需要的信息 |
|----------|------|----------------|
| 本地 | `/backup/` | 无。dst 是**父目录**，源会以其 basename 创建子目录 |
| 远程 ncp | `ncp://server:9900/backup/` | 确认远程 `ncp serve` 已运行。dst 是父目录 |
| OSS | `oss://bucket/backup/` | endpoint、region、AK、SK。dst 是父目录 |

**路径语义：** `ncp copy /data/project /backup/` 的结果在 `/backup/project/...`，不是 `/backup/...`。dst 是父目录，basename 由 ncp 自动添加。如果用户说"复制到 /backup/project"，实际应给 dst = `/backup/`。

**如果目标是 `ncp://`：**
- 确认远程服务器已运行 `ncp serve`
- 提示："远程 ncp 服务器是否已启动？如未启动，需先运行：`ncp serve --base <dir> --listen :9900`"

**如果目标是 `oss://`：**
- 收集全部四个 OSS 参数：
  - `--endpoint`（如 `oss-cn-shenzhen.aliyuncs.com`）
  - `--region`（如 `cn-shenzhen`）
  - `--access-key-id`
  - `--access-key-secret`
- 一次性询问，不要逐个追问

**如果源是 `oss://`：**
- 同样需要这四个 OSS 参数

## 1.3 评估性能参数

根据场景为 CopyParallelism 和 ChannelBuf 推荐合理值。需要收集以下信息：

### 1.3.1 收集场景信息

向用户询问以下信息（可合并为一次提问）：

1. **文件数量级** — 大约多少文件？（<1K / 1K~100K / 100K~1M / >1M）
2. **是否有大文件** — 是否有超过 100MB 的文件？
3. **源端存储介质** — SSD / HDD / OSS？
4. **目标端存储介质** — SSD / HDD / ncp:// / OSS？

如果用户不清楚，根据源/目标路径合理推断：
- 本地路径 → 可执行 `lsblk -d -o name,rota` 或 `diskutil info` 判断 SSD/HDD
- `oss://` → OSS
- `ncp://` → 网络目标

### 1.3.2 查询机器配置

自动检测运行 ncp 的机器配置，无需询问用户：

```bash
# CPU 核心数
sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4

# 可用内存（MB）
vm_stat | head -10 2>/dev/null || free -m 2>/dev/null || echo 4096
```

从输出提取 cpu 数量和可用内存。

### 1.3.3 计算推荐参数

**CopyParallelism** — 根据存储介质和文件数量：

| 源介质 | 目标介质 | 文件数 < 1K | 文件数 ≥ 1K | 最大值 |
|--------|---------|-------------|-------------|--------|
| SSD | SSD | 2 | 4~cpu×2 | cpu×2 |
| SSD | HDD/网络/OSS | 2 | 2~4 | 4 |
| HDD | 任意 | 2 | 2 | 2 |
| 任意 | ncp:// | 2 | 4~8 | 8 |

文件数 ≥ 1K 时，SSD→SSD 场景取 `min(4 + 额外, cpu×2)`，其中额外值按文件量递增：
- 1K~100K：4
- 100K~1M：8
- >1M：min(cpu×2, 16)

**ChannelBuf** — 根据可用内存：

| 可用内存 | ChannelBuf |
|----------|------------|
| < 2GB | 100,000 |
| 2~4GB | 500,000 |
| 4~16GB | 1,000,000 |
| > 16GB | 2,000,000 |

### 1.3.4 其他可选参数

仅在用户未指定时才考虑，不要主动询问：

| 参数 | 默认值 | 何时询问 |
|------|--------|----------|
| `--cksum-algorithm` | md5 | 仅当用户提到 xxhash 或性能关注时 |
| `--skip-by-mtime` | true（启用） | 仅当用户想强制全量复制时 |
| `--enable-SyncWrites` | true | 仅当用户追求极致速度且可接受数据丢失风险时 |
| `--enable-DirectIO` | false | 仅当用户有特定 IO 需求时 |
| `--FileLogInterval` | 5 | 通常默认即可 |

**不要询问用户未提及的参数。** 使用默认值即可。

## 1.4 汇总并继续

展示收集到的参数摘要，包含性能推荐：

```
复制计划：
  源：        /data/project (SSD)
  目标：      ncp://server:9900/backup
  文件数量：  ~500 万
  并行数：    8（cpu×2=16，ncp:// 目标上限 8）
  ChannelBuf：2,000,000（可用内存 32GB）
  校验算法：  md5
  跳过策略：  skip-by-mtime（默认启用）
```

用户确认后，进入步骤 2。

完整阅读并遵循：`./steps/step-02-prepare-and-execute.md`
