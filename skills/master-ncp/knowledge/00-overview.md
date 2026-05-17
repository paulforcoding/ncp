# ncp 概览

## 触发条件

当以下情况出现时读这个文件：
- 首次使用 ncp，不了解 ncp 是什么
- 需要向用户解释 ncp 的定位和用途
- 不确定 ncp 与 cp、rsync 等工具的区别

## 快速行动

ncp 是面向企业海量文件复制的命令行工具，核心能力：DB 记录进度实现断点续传、结构化 NDJSON 输出供 Agent 监控、跨服务器和云存储复制。

ncp 不是 cp 的替代品——它是海量场景（千万级文件、TB/PB 级数据）下的专用复制工具，核心设计假设是"中断是常态"。

## 详情

### ncp 解决什么问题

| 问题 | ncp 的解法 |
|------|-----------|
| 千万级文件遍历一次代价极高 | DB 记录，一次任务只遍历一次，断点续传成本极低 |
| 复制中断后无法知道哪些已完成 | PebbleDB 持久化每个文件的复制状态 |
| 运维无法实时盯屏 | NDJSON FileLog 供 Agent 程序化消费，Agent 只在出错时通知人类 |
| 跨服务器/跨云复制 | 统一协议支持本地、ncp://、oss://、cos://、obs:// |

### 支持的操作

| 命令 | 作用 |
|------|------|
| `ncp copy <src>... <dst>` | 复制文件（多源 → 单目标） |
| `ncp cksum <src> <dst>` | 校验源和目标的数据一致性 |
| `ncp resume <taskID>` | 恢复中断任务 |
| `ncp serve` | 启动 ncp 协议服务器（单 task，完成后自动退出） |
| `ncp task list/show/delete` | 任务管理 |

### 常见使用场景

以下场景均以远程服务器 `192.168.1.188` 为例。每个 ncp 命令执行后，Agent 必须立即监控 stderr/stdout、ProgramLog、FileLog 三条通道，并记录 taskId。FileLog 中三种事件（file_complete、file_metadata_complete、progress_summary）的详细格式见 `20-concepts/log.md`。

#### 场景一：本地 → 远程，目的端无源端文件

步骤一：登录远程服务器，启动 serve：
```bash
ssh 192.168.1.188 "nohup ncp serve --listen :9900 </dev/null >>/tmp/ncp-serve.log 2>&1 &"
```
若服务器无 ncp 程序或登录有障碍，告知用户自行处理。

步骤二：本地执行 copy：
```bash
ncp copy <src> ncp://192.168.1.188:9900/<dst> \
  --FileLogOutput /tmp/ncp_file.log --FileLogInterval 10 \
  --ProgramLogOutput /tmp/ncp_program.log --ProgramLogLevel warn
```
执行后立即监控三条 log 通道，记录 taskId。

步骤三：从 FileLog 中收到 `progress_summary` 的 `finished=true` 信号，告知用户复制完成，展示复制任务总结报告，询问用户是否继续做 `ncp cksum` 检查完整性。

步骤四（可选）：再次在远程启动 serve（copy 完成后 serve 已退出），然后基于已有 task 执行 cksum：
```bash
ncp cksum --task <taskId> --no-skip-by-mtime \
  --FileLogOutput /tmp/ncp_file.log --FileLogInterval 10 \
  --ProgramLogOutput /tmp/ncp_program.log --ProgramLogLevel warn
```
`--task` 模式从已有 task 的 meta.json 读取 src/dst，不需要再传路径参数。**必须加 `--no-skip-by-mtime`**，否则 mtime+size 匹配会导致 cksum 瞬间全部跳过，无法真正校验数据完整性。执行后立即监控三条 log 通道。

步骤五（可选）：从 FileLog 中收到 cksum 完成信号，告知用户 cksum 完成，展示 cksum 总结报告。检查 FileLog 中是否有 `result: "mismatch"` 的文件，有的话告知用户。

#### 场景二：本地 → 远程，目的端已有部分文件，不确定哪些需要复制

步骤一：登录远程服务器，启动 serve（同场景一）。

步骤二：先 cksum，找出哪些文件不一致：
```bash
ncp cksum <src> ncp://192.168.1.188:9900/<dst> \
  --FileLogOutput /tmp/ncp_file.log --FileLogInterval 10 \
  --ProgramLogOutput /tmp/ncp_program.log --ProgramLogLevel warn
```
执行后立即监控三条 log 通道。

步骤三：从 FileLog 中收到 cksum 完成信号，告知用户 cksum 结果：本地多少个文件、目的端多少个文件、有多少个文件 mismatch（需要复制）。

步骤四：再次在远程启动 serve（cksum 完成后 serve 已退出），基于已有 task 执行 copy：
```bash
ncp copy --task <taskId> \
  --FileLogOutput /tmp/ncp_file.log --FileLogInterval 10 \
  --ProgramLogOutput /tmp/ncp_program.log --ProgramLogLevel warn
```
`--task` 模式从已有 task 的 meta.json 读取 src/dst，不需要再传路径参数。ncp 会跳过 CksumStatus=pass 的文件，只复制 mismatch 的文件。执行后立即监控三条 log 通道。

步骤五：从 FileLog 中收到 copy 完成信号，告知用户复制完成，展示总结报告，询问是否再做一次 cksum 确认完整性。

#### 场景三：复制或 cksum 任务中断

```bash
ncp resume <taskID>
```
执行后立即监控三条 log 通道，定期向用户报告进度。若目标为 `ncp://`，resume 前需先重启远端 serve。

### 关键设计约束

- ncp serve 在单 task 完成后立即退出（非闲置超时），后续 cksum/resume 需重启 serve
- 云 URL 必须带 `<profile>@` 前缀，profile 定义在 `ncp_config.json`
- copy 路径语义对齐 `cp`（dst 不存在时复制 AS dst，dst 存在时复制 INTO dst）

## 关联文件

- INDEX.md — 知识库总索引
- 20-concepts/log.md — FileLog 三种事件的详细格式与监控方案
