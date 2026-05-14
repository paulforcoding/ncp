# Bug #001: ncp 协议 OpenMsg.Flags 跨平台不兼容

## 严重程度

**Critical** — 跨平台远程复制（macOS → Linux / Linux → macOS）100% 失败。

## 现象

从 macOS 客户端执行 `ncp copy /tmp/src ncp://linux-server:9900/tmp/dst` 时，所有文件写入失败：

```
remote openfile src/file.txt: server error: code=0x1001 msg=open /tmp/dst/src/file.txt: no such file or directory
```

目录创建（MsgMkdir）成功，但文件创建（MsgOpen）全部失败。同平台（macOS↔macOS 或 Linux↔Linux）正常。

## 根因

`OpenMsg.Flags` 直接传递了 `os.O_*` 常量的值，但 **这些常量在 macOS 和 Linux 上数值不同**：

| 常量 | macOS | Linux |
|------|-------|-------|
| `O_WRONLY` | `0x1` | `0x1` |
| `O_CREAT` | `0x200` | `0x40` |
| `O_TRUNC` | `0x400` | `0x200` |
| `O_APPEND` | `0x8` | `0x400` |

macOS 客户端发送 `O_WRONLY|O_CREAT|O_TRUNC = 0x601`，Linux 服务端将其解码为：

- `0x601 & 0x1` → O_WRONLY ✓
- `0x601 & 0x40` → **O_CREAT 未设置** ✗
- `0x601 & 0x200` → O_TRUNC ✓
- `0x601 & 0x400` → O_APPEND（误设）✗

没有 `O_CREAT`，`os.OpenFile` 对不存在的文件返回 `ENOENT`。

## 涉及文件

- `pkg/impls/storage/remote/destination.go:60` — 客户端发送 `uint32(os.O_WRONLY | os.O_CREATE | os.O_TRUNC)`
- `internal/ncpserver/handler.go:187` — 服务端 `os.OpenFile(fullPath, int(msg.Flags), ...)`
- `internal/protocol/message.go:36-42` — `OpenMsg.Flags` 字段定义（无文档说明跨平台约束）

## 修复方案

### 方案：协议层定义平台无关的 Flag 常量

1. **在 `internal/protocol/` 中定义协议级 Flag 常量**：

```go
// Protocol-level OpenMsg flags (platform-independent)
const (
    ProtoO_RDONLY = 0x00
    ProtoO_WRONLY = 0x01
    ProtoO_RDWR   = 0x02
    ProtoO_CREAT  = 0x04
    ProtoO_TRUNC  = 0x08
    ProtoO_APPEND = 0x10
)
```

2. **客户端发送时翻译**（`remote/destination.go`）：

```go
func osFlagsToProto(flags int) uint32 {
    var pf uint32
    if flags&os.O_WRONLY != 0 { pf |= ProtoO_WRONLY }
    if flags&os.O_RDWR != 0   { pf |= ProtoO_RDWR }
    if flags&os.O_CREAT != 0  { pf |= ProtoO_CREAT }
    if flags&os.O_TRUNC != 0  { pf |= ProtoO_TRUNC }
    if flags&os.O_APPEND != 0 { pf |= ProtoO_APPEND }
    return pf
}
```

3. **服务端接收时翻译**（`ncpserver/handler.go`）：

```go
func protoFlagsToOS(pf uint32) int {
    var flags int
    if pf&ProtoO_WRONLY != 0 { flags |= os.O_WRONLY }
    if pf&ProtoO_RDWR != 0   { flags |= os.O_RDWR }
    if pf&ProtoO_CREAT != 0  { flags |= os.O_CREAT }
    if pf&ProtoO_TRUNC != 0  { flags |= os.O_TRUNC }
    if pf&ProtoO_APPEND != 0 { flags |= os.O_APPEND }
    return flags
}
```

4. **MsgOpen 的 Flags 字段语义变更**：从"原始 os 标志"改为"协议标志"。需要同步更新 `protocol/message.go` 的注释。

5. **版本兼容性**：由于 Magic 已包含版本号（当前 Version=2），此修改应将 Version 升至 3，旧客户端连新版 server 时在 MsgInit 阶段拒绝。Version 3 升级同时包含 Bug #002（POSIX 元数据丢失修复），见 `002-protocol-posix-metadata-loss.md`。

## 文档修正

CLAUDE.md 和 skills 中对 ncp serve 的描述 "单客户端" 含义模糊，应改为 "单 task"（一个 ncp 客户端进程可以有多个 replicator 并行连接，这是正常行为）：

- `CLAUDE.md:22`：`单 task、单客户端、单模式` → `单 task、单模式`
- `skills/master-ncp/SKILL.md:22`：`单客户端、单模式` → `单模式`
