# ncp serve

## 触发条件

当以下情况出现时读这个文件：
- 需要在远程服务器上启动 ncp 协议服务
- 不确定 serve 的参数和生命周期
- copy/cksum/resume 目标为 ncp:// 时连接失败
- 不确定 serve 何时退出

## 快速行动

```bash
# 在远程服务器上启动 serve
ncp serve --listen :9900

# 通过 SSH 后台启动
ssh <host> "nohup ncp serve --listen :9900 </dev/null >>/tmp/ncp-serve.log 2>&1 &"
```

serve 只有 `--listen` 和 `--serve-temp-dir` 两个 flag，**没有** `--base` 等其他 flag。

## 详情

### 生命周期（关键）

ncp serve 是**单 task、单模式**的服务器：
1. 启动后监听指定端口，等待客户端连接
2. 客户端通过 MsgInit 消息携带 Mode（copy/cksum）和 basePath
3. serve 为该 task 服务直到完成
4. **收到 MsgTaskDone 后立即退出**（不是闲置超时）
5. 退出后清理 walker DB

**这意味着：** 每次 copy/cksum 任务完成后，serve 就退出了。后续如果需要对同一目标执行 cksum 或 resume，**必须先重启 serve**。

### 完整 flags

| Flag | 默认值 | 说明 |
|------|--------|------|
| `--listen` | :9900 | 监听地址（host:port） |
| `--serve-temp-dir` | /tmp/ncpserve | walker DB 临时目录 |

### 安全提示

MVP 阶段使用明文 TCP，无加密。**仅限内网/VPN 使用**，不要暴露到公网。

### 常见问题

| 问题 | 原因 | 解决 |
|------|------|------|
| `connection refused` | serve 未启动或已退出 | 重启 serve：`ssh <host> "nohup ncp serve --listen :9900 ..."` |
| `unknown flag "--base"` | serve 没有 --base flag | basePath 由客户端通过 MsgInit 消息携带 |
| serve 进程不存在 | 上一次 task 完成后 serve 已退出 | 这是正常行为，重新启动即可 |

### Agent 自动处理

当目标为 `ncp://` 时，Agent 应自动检测 serve 是否在运行，不要询问用户。如果连接失败，自动通过 SSH 重启 serve。

## 关联文件

- copy.md — 执行 copy 前需确保 serve 已启动
- cksum.md — cksum 目标为 ncp:// 时需重启 serve
- resume.md — resume 目标为 ncp:// 时需重启 serve
