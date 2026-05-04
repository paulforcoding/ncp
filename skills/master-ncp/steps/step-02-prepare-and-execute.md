# 步骤 2：准备与执行

构建 ncp 命令、获用户确认、执行并实时监控进度。

## 2.1 构建命令

根据步骤 1 收集的参数构建完整的 `ncp copy` 命令。

**命令模板：**

```bash
ncp copy <src1> [<src2> ...] <dst_parent> \
  [--CopyParallelism N] \
  [--ChannelBuf N] \
  [--cksum-algorithm md5|xxh64] \
  [--ProgressStorePath /tmp/ncp_progress_store] \
  [--FileLogOutput /tmp/ncp_file_log.json] \
  [--FileLogInterval 5] \
  [--endpoint OSS_ENDPOINT] \
  [--region OSS_REGION] \
  [--access-key-id OSS_AK] \
  [--access-key-secret OSS_SK] \
  [--no-skip-by-mtime]   # 仅当用户要禁用跳过时
```

**注意 dst 语义：** `dst` 是**父目录**，源会以其 basename 创建子目录。例如 `ncp copy /data/project /backup/` 的结果在 `/backup/project/...`。

**注意事项：**
- 只包含与默认值不同或必需的参数（如 OSS 凭据）
- 始终包含 `--ProgressStorePath` 和 `--FileLogOutput`，以便监控进度
- 将 `--FileLogOutput` 设为已知文件路径（默认：`/tmp/ncp_file_log.json`）— 这是监控进度的关键
- 将 `--FileLogInterval` 设为合理值（默认 5 秒即可；超大规模任务可用 10 以减少噪音）

## 2.2 用户确认

展示完整命令并请求确认（注意 dst 是父目录）：

```
我将运行以下命令：

  ncp copy /data/project ncp://server:9900/backup/ \
    --CopyParallelism 8 \
    --ChannelBuf 2000000 \
    --ProgressStorePath /tmp/ncp_progress_store \
    --FileLogOutput /tmp/ncp_file_log.json \
    --FileLogInterval 5

结果将在 ncp://server:9900/backup/project/... 下
是否执行？（是/否）
```

**用户确认前不要执行。**

## 2.3 执行

在后台运行 ncp copy 命令：

```bash
ncp copy ... &
```

使用 Bash 工具的 `run_in_background: true`。

**捕获 taskID：** ncp 启动后会将 taskID 写入 ProgressStorePath 下的任务目录。从中获取最新 taskID：

```bash
# 从 ProgressStorePath 获取最新任务 ID
ls -t /tmp/ncp_progress_store | head -1
```

如果进程立即失败（退出码 != 0），从输出诊断错误并报告给用户。

## 2.4 监控进度

开始监控 FileLog 中的 `progress_summary` 事件。使用 Monitor 工具 tail 文件并过滤进度事件：

```bash
tail -f /tmp/ncp_file_log.json | grep --line-buffered '"progress_summary"'
```

每收到一个 `progress_summary` 事件，以人性化格式报告给用户：

```
进度：480,000 / 1,000,000 文件 (48.0%)
  已复制：   480,000 文件，100.0 GB
  速度：     3,201 文件/秒，715.8 MB/秒
  失败：     3 文件
  遍历：     完成（已发现 1,000,000）
```

**格式化数字以提高可读性：**
- 文件数：使用逗号分隔（如 `1,000,000`）
- 字节数：转换为易读单位（KB、MB、GB、TB）
- 百分比：保留一位小数
- 速度：根据数量级选择合适单位

**检测完成：** `progress_summary` 事件中 `finished=true` 时，任务完成。停止监控并进入步骤 3。

## 2.5 健康监控

除了进度监控外，必须持续监控以下健康指标。建议每 60 秒检查一次。

### 2.5.1 磁盘空间

监控 ProgressStore、FileLog、ProgramLog 所在文件系统的可用空间。

```bash
# 检查关键路径所在文件系统的可用空间
df -h /tmp/ncp_progress_store /tmp/ncp_file_log.json 2>/dev/null | tail -n +2
```

**处理策略：**

| 可用空间 | 动作 |
|----------|------|
| > 10% | 正常，无需干预 |
| 5~10% | 警告用户："磁盘空间不足（X% 可用），复制可能中断" |
| < 5% | 立即 kill ncp 进程，询问用户如何处理（见下方） |

**磁盘满是不可自动恢复的** — ncp 可能已写入不完整数据。遇到磁盘 < 5% 时：

1. Kill ncp 进程
2. 向用户报告情况："磁盘空间已不足 X%，ncp 已停止。taskID 已保存，可安全 resume。"
3. 提供选项让用户决定：
   - 清理磁盘空间后 `ncp resume <taskID>`
   - 将 ProgressStore/FileLog 移到其他磁盘后 `ncp resume <taskID> --ProgressStorePath /new/path`
   - 放弃本次任务

### 2.5.2 卡住检测

通过 FileLog 的 `progress_summary` 事件检测 ncp 是否卡住。

**判断标准：** 连续 N 个 `progress_summary` 间隔内（N = 3，即约 15 秒 @ FileLogInterval=5），以下指标全部无变化：
- `replicator.filesCopied` 不变
- `replicator.bytesCopied` 不变
- `dbWriter.totalProcessed` 不变

且 `finished=false`，则判定为卡住。

**注意区分：** Walker 完成但 Replicator 还在处理大文件时，`filesCopied` 可能短暂不变但 `bytesCopied` 在增长 — 这不算卡住。

**卡住时的处理策略：**

1. Kill ncp 进程
2. 检查可能原因：
   - 网络 ncp:// 目标连接断开？→ 检查 `ncp serve` 是否还在运行
   - OSS 限流？→ 检查 OSS bucket 的 QPS/带宽限制
   - 目标磁盘 IO 阻塞？→ 检查目标端 iostat
3. 自动恢复：降低 `--CopyParallelism`（减半），然后用 `ncp resume <taskID>` 恢复
4. 如果降低并行数后仍卡住，报告给用户，询问是否继续尝试

### 2.5.3 内存使用

监控 ncp 进程的内存占用。

```bash
# 获取 ncp 进程的 RSS（实际内存占用）
ps -o rss= -p <ncp_pid> 2>/dev/null
```

**判断标准：**

| 情况 | 动作 |
|------|------|
| RSS > 可用内存的 80% | 警告用户："ncp 内存占用已达可用内存的 X%，接近 OOM 风险" |
| RSS > 可用内存的 90% | Kill ncp，降低 ChannelBuf 后 resume |

**自动恢复流程：**
1. Kill ncp 进程
2. 按以下规则降低 ChannelBuf：
   - 当前 > 500,000 → 降到 500,000
   - 当前 > 100,000 → 降到 100,000
   - 当前 = 100,000 → 无法再降，报告给用户
3. 用新参数 `ncp resume <taskID> --ChannelBuf <新值>` 恢复

### 2.5.4 健康检查周期

- 磁盘空间：每 60 秒检查一次
- 卡住检测：通过 progress_summary 事件实时检测（每次收到事件时比对）
- 内存使用：每 60 秒检查一次

可用 CronCreate 或在 Monitor 回调中嵌入检查逻辑。

## 2.6 处理中断

如果 ncp 进程被中断或用户取消：

1. taskID 已保存在进度存储中（`--ProgressStorePath`）
2. 告诉用户："复制被中断。稍后可通过 `ncp resume <taskID>` 恢复"
3. 报告中断前的进度

## 2.7 处理错误

如果 `progress_summary` 显示 `totalFailed > 0` 或 `exitCode == 2`：

1. 不要报告为完全失败 — 部分文件可能已成功
2. 同时报告成功和失败的数量
3. 建议后续操作：
   - `ncp resume <taskID>` — 重试失败的文件
   - `ncp cksum --task <taskID>` — 验证已成功的文件
   - `ncp cksum /data/project /backup/project` — 独立校验（两端为显式基址）
   - 查看 FileLog 中具体失败文件的详情

任务完成（无论成功还是有错误）后，进入步骤 3。

完整阅读并遵循：`./steps/step-03-report.md`
