# 步骤 1：收集需求

收集构建 ncp 命令所需的所有信息。使用 AskUserQuestion 进行结构化选择，并根据需要追问澄清。

## 1.1 确定源路径

向用户询问源路径。ncp 支持多个源（仅限本地路径可多源混用；云存储和 ncp:// 不支持混用）。

- **本地路径**：如 `/data/project`、`/home/user/docs`
- **ncp://**：如 `ncp://server:9900/data/`（可作为源或目标）
- **OSS 路径**：如 `oss://prod@my-bucket/data/`(必须带 `<profile>@` 前缀,见 1.2)
- **COS 路径**：如 `cos://prod@my-bucket-1250000000/data/`(必须带 `<profile>@` 前缀,见 1.2)
- **OBS 路径**：如 `obs://prod@my-bucket/data/`(必须带 `<profile>@` 前缀,见 1.2)

如果用户提供本地路径，先验证路径是否存在。

## 1.2 确定目标

向用户询问目标。协议决定后端类型：

| 目标类型 | 示例 | 额外需要的信息 |
|----------|------|----------------|
| 本地 | `/backup/` | 无。dst 是**父目录**，源会以其 basename 创建子目录 |
| 远程 ncp | `ncp://server:9900/backup/` | 确认远程 `ncp serve` 已运行。dst 是父目录 |
| OSS | `oss://prod@bucket/backup/` | URL 必须带 `<profile>@`,profile 在 `ncp_config.json` 中已定义。dst 是父目录 |
| COS | `cos://prod@bucket-1250000000/backup/` | URL 必须带 `<profile>@`,profile 在 `ncp_config.json` 中已定义。dst 是父目录 |
| OBS | `obs://prod@bucket/backup/` | URL 必须带 `<profile>@`,profile 在 `ncp_config.json` 中已定义。dst 是父目录 |

**路径语义：**`ncp copy /data/project /backup/` 的结果在 `/backup/project/...`，不是 `/backup/...`。dst 是父目录，ncp 自动以源 basename 创建子目录。向用户展示时直接说出最终落点，如"目标写 `ncp://host:port/root/`，文件最终落在 `/root/crewAI/` 下"。

**如果源或目标是 `ncp://`：**
- **不得询问用户** ncp serve 是否已启动、本机 ncp 二进制是否已编译 — 自行检测。
- 检测远程 serve 是否在线：`nc -z <host> <port>` 或 `timeout 2 bash -c "echo >/dev/tcp/<host>/<port>" 2>&1`。
- 若 serve 不在线：自行将 ncp 交叉编译为 Linux amd64 版本（`GOOS=linux GOARCH=amd64 go build -o ncp-linux ./cmd/ncp/`），scp 部署到远程，然后 `ssh <host> "nohup ncp serve --listen :<port> > /tmp/ncp-serve.log 2>&1 &"`。
- 本机二进制不存在则执行 `make build`。
- 远程 `ncp serve` 的参数只有 `--listen`（默认 :9900）和 `--serve-temp-dir`（默认 /tmp/ncpserve），**没有 `--base`**。目标路径由客户端在 URL 中指定（`ncp://host:port/path`）。

**如果源或目标是 `oss://`、`cos://` 或 `obs://`：**

不要再询问 endpoint/region/AK/SK,这些已经从配置中读取。改为按以下流程确认 profile:

1. 询问用户预期使用哪个 profile 名(例如 "prod"、"dr"、"acct-a"、"cos-prod"、"obs-prod")。如果用户不确定,先运行 `ncp config show` 查看当前生效的 profile 列表,再让用户挑选。
2. 用 `ncp config show --profile <name>` 验证该 profile 已存在且字段齐全(Provider/Region/AK/SK 都有值,Endpoint 对 OSS/OBS 必填、对 COS 可选,AK/SK 会脱敏显示首尾各 4 位)。
3. 如果 `ncp config show --profile <name>` 报 "not found":
   - 解释 profile 必须先在 `ncp_config.json` 的 `Profiles` 下定义,字段名为 `Provider`/`Endpoint`/`Region`/`AK`/`SK`。`oss://` 要求 `Provider == "oss"`; `cos://` 要求 `Provider == "cos"`; `obs://` 要求 `Provider == "obs"`。
   - 引导用户写入合适的 config(`/etc/ncp_config.json`、`~/ncp_config.json`、`./ncp_config.json` 任选一层),或在最高优先级层临时写入。
   - 推荐用 `${env:VAR}` 占位符引用环境变量,避免明文密钥落盘;若必须明文,提醒文件 mode 必须为 `0600`,否则 ncp 拒绝启动。
   - COS 的 `Endpoint` 可选,如果不填会从 `Region+Bucket` 自动构造为 `https://<bucket>.cos.<region>.myqcloud.com`。
   - OBS 的 `Endpoint` 在 profile 校验阶段为必填,但运行期若缺省 SDK 会回退到 `https://obs.<region>.myhuaweicloud.com`。
   - 用户写完后,再次执行 `ncp config show --profile <name>` 验证。
4. 跨账号场景下分别确认 src/dst 各自要用哪个 profile,URL 形如 `oss://acct-a@src/...`、`oss://acct-b@dst/...`,`cos://acct-a@src/...`、`cos://acct-b@dst/...` 或 `obs://acct-a@src/...`、`obs://acct-b@dst/...`。

**一次性询问 profile 名,不要逐字段追问 endpoint/region/AK/SK。这些信息从配置文件读,不再走 CLI。**

## 1.3 评估性能参数

根据场景为 CopyParallelism 和 ChannelBuf 推荐合理值。需要收集以下信息：

### 1.3.1 收集场景信息

向用户询问以下信息（可合并为一次提问）：

1. **文件数量级** — 大约多少文件？（<1K / 1K~100K / 100K~1M / >1M）
2. **是否有大文件** — 是否有超过 100MB 的文件？
3. **源端存储介质** — SSD / HDD / OSS / COS / OBS？
4. **目标端存储介质** — SSD / HDD / ncp:// / OSS / COS / OBS？

如果用户不清楚，根据源/目标路径合理推断：
- 本地路径 → 可执行 `lsblk -d -o name,rota` 或 `diskutil info` 判断 SSD/HDD
- `oss://` → OSS
- `cos://` → COS
- `obs://` → OBS
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
| SSD | HDD/网络/OSS/OBS | 2 | 2~4 | 4 |
| HDD | 任意 | 2 | 2 | 2 |
| 任意 | ncp:// | 2 | 4~8 | 8 |
| 任意 | COS | 2 | 2~4 | 4 |

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
| `--FileLogInterval` | 10 | 技能统一设为 10 秒，平衡可见性与日志量 |

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
