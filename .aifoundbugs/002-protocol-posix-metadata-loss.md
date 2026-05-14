# Bug #002: ncp 协议丢失 POSIX 元数据（UID/GID/chown/chmod/xattr/setuid/setgid/sticky）

## 严重程度

**High** — 通过 `ncp://` 协议复制时，所有权和扩展属性被静默丢弃，造成数据保真度降级。与 OSS 后端（完整保存 POSIX 元数据）形成不一致。

## 现象

```bash
# 本地源文件
$ ls -la /data/project/
drwxr-sr-x  2 app app  4096 May 10 12:00 config/    # setgid + group=app
-rwsr-xr-x  1 root root 8192 May 10 12:00 run       # setuid + owner=root
-rw-r--r--  1 app app  1024 May 10 12:00 data.txt

# 复制到远程 ncp server
$ ncp copy /data/project ncp://server:9900/backup/

# 远程结果
$ ls -la /backup/project/
drwxr-xr-x  2 ncp ncp  4096 May 10 12:00 config/    # setgid 丢失, owner=ncp 进程用户
-rwxr-xr-x  1 ncp ncp  8192 May 10 12:00 run         # setuid 丢失, owner=ncp 进程用户
-rw-r--r--  1 ncp ncp  1024 May 10 12:00 data.txt    # owner=ncp 进程用户

# xattr 也全部丢失
$ getfattr -d /data/project/data.txt
user.comment="important"
$ getfattr -d /backup/project/data.txt
(空)
```

## 根因分析

### 问题 1: UID/GID 被服务端忽略

**客户端正确发送了 UID/GID**，但服务端完全忽略：

- `pkg/impls/storage/remote/destination.go:58-64` — `OpenMsg{UID: uint32(uid), GID: uint32(gid)}` 正确发送
- `pkg/impls/storage/remote/destination.go:78-84` — `MkdirMsg{UID: uint32(uid), GID: uint32(gid)}` 正确发送
- `internal/ncpserver/handler.go:187` — `os.OpenFile(fullPath, int(msg.Flags), os.FileMode(msg.Mode))` — **UID/GID 未使用**
- `internal/ncpserver/handler.go:302` — `os.MkdirAll(fullPath, os.FileMode(msg.Mode))` — **UID/GID 未使用**

协议中没有 `MsgChown` 消息，`SetMetadata` 也不发送 chown 操作。

### 问题 2: setXattr 是 no-op

- `internal/ncpserver/handler.go:470-474`:
  ```go
  func setXattr(path, key, value string) error {
      return nil  // 静默丢弃所有 xattr！
  }
  ```
- 客户端 `remote/destination.go:132-141` 正确发送了 `MsgSetxattr`，但服务端返回成功而实际未设置
- 所有 xattr（`user.*`、`security.*`、macOS `com.apple.*` 等）均被静默丢弃

### 问题 3: setuid/setgid/sticky 位丢失

- `internal/ncpserver/walker.go:173`: `Mode: uint32(mode.Perm())` — 仅保留低 9 位（rwxrwxrwx）
- Go 的 `os.FileMode.Perm()` 返回 `m & 0o777`，丢弃 `ModeSetuid`(0o4000)、`ModeSetgid`(0o2000)、`ModeSticky`(0o1000)
- 客户端 `local/source.go:86`: 同样使用 `mode.Perm()`，DiscoverItem 的 Mode 本身就不包含这些位
- 结果：setuid/setgid/sticky 在 Walker 阶段就已丢失，连发送的机会都没有

### 问题 4: ListEntry 不携带 UID/GID

- `internal/protocol/message.go:507`: `ListEntry` 只有 RelPath/FileType/FileSize/Mode/Mtime/LinkTarget/ETag
- 注释明确写道：`No uid/gid for remote scenarios`
- `internal/ncpserver/walker.go:169-183`: `infoToListEntry` 不填充 UID/GID
- `pkg/impls/storage/remote/source.go:184`: `listEntryToDiscoverItem` 将 UID/GID 设为 0
- 结果：ncp:// 作为源时，所有权信息不可用

## 跨存储对比

| 属性 | local→local | local→OSS | local→ncp:// | ncp://→local |
|------|------------|-----------|--------------|-------------|
| permission bits | ✓ | ✓ (header) | ✓ (但受 Bug#001 影响) | ✓ |
| setuid/setgid/sticky | ✓ | ✓ (header) | ✗ 丢失 | ✗ 丢失 |
| UID/GID | ✓ | ✓ (header) | ✗ 忽略 | ✗ 不可用 |
| xattr | ✓ | ✓ (header) | ✗ no-op | N/A |
| mtime | ✓ | ✓ (header) | ✓ | ✓ |
| symlink | ✓ | ✓ (header) | ✓ | ✓ |

OSS 后端通过自定义 header 完整保存了 POSIX 元数据，而 ncp 协议则大面积丢失。

## 涉及文件

### 协议层
- `internal/protocol/message.go:36-42` — `OpenMsg` 的 Flags/Mode/UID/GID 字段
- `internal/protocol/message.go:174-181` — `MkdirMsg` 的 Mode/UID/GID 字段
- `internal/protocol/message.go:507-515` — `ListEntry` 缺少 UID/GID/setuid 位
- `internal/protocol/frame.go:12` — Version 需要从 2 升至 3

### 服务端
- `internal/ncpserver/handler.go:187` — OpenMsg UID/GID 被忽略
- `internal/ncpserver/handler.go:302` — MkdirMsg UID/GID 被忽略
- `internal/ncpserver/handler.go:470-474` — setXattr 是 no-op
- `internal/ncpserver/walker.go:157-184` — infoToListEntry 丢失 setuid 位和 UID/GID

### 客户端
- `pkg/impls/storage/remote/destination.go:60` — OpenMsg.Flags 发送原始 os.O_* 值 (Bug #001)
- `pkg/impls/storage/remote/destination.go:108-144` — SetMetadata 只处理 utime 和 xattr，不处理 chown/chmod
- `pkg/impls/storage/remote/source.go:184-199` — listEntryToDiscoverItem UID/GID=0，Mode 丢失高位

### 本地存储（数据源头）
- `pkg/impls/storage/local/source.go:86` — `mode.Perm()` 丢弃 setuid/setgid/sticky
- `pkg/impls/storage/local/source.go:120` — Reader attr 也用 `mode.Perm()`

## 修复方案

### 协议层修改（Version 3，与 Bug #001 一起升级）

#### 1. 协议级 Open Flags（Bug #001，此处不重复）

#### 2. Mode 字段：携带完整 permission + special bits

**问题**: `mode.Perm()` 只保留低 9 位。Go 的 `os.FileMode` 用 bit 12/13/14 表示 setuid/setgid/sticky，但这些是 Go 抽象，不是 POSIX mode bit 的直接映射。

**方案**: 协议 Mode 字段用低 12 位表达完整 POSIX 权限：

```go
// 协议级 Mode 位定义（与 POSIX stat.st_mode 权限位一致）
const (
    ProtoModeSetuid  uint32 = 0o4000
    ProtoModeSetgid  uint32 = 0o2000
    ProtoModeSticky  uint32 = 0o1000
    ProtoModeOwnerR  uint32 = 0o0400
    ProtoModeOwnerW  uint32 = 0o0200
    ProtoModeOwnerX  uint32 = 0o0100
    ProtoModeGroupR  uint32 = 0o0040
    ProtoModeGroupW  uint32 = 0o0020
    ProtoModeGroupX  uint32 = 0o0010
    ProtoModeOtherR  uint32 = 0o0004
    ProtoModeOtherW  uint32 = 0o0002
    ProtoModeOtherX  uint32 = 0o0001
)
```

这些数值在所有 POSIX 系统（Linux/macOS/*BSD）上完全一致，因为它们就是 POSIX 标准定义的。

**客户端翻译**（发送时 — 从 Go FileMode 转 POSIX 数值）：
```go
func osModeToProto(mode os.FileMode) uint32 {
    var pm uint32 = uint32(mode.Perm()) // 低 9 位: rwxrwxrwx
    if mode&os.ModeSetuid != 0 {
        pm |= 0o4000 // POSIX S_ISUID
    }
    if mode&os.ModeSetgid != 0 {
        pm |= 0o2000 // POSIX S_ISGID
    }
    if mode&os.ModeSticky != 0 {
        pm |= 0o1000 // POSIX S_ISVTX
    }
    return pm
}
```

**服务端使用**（接收时 — POSIX 数值直接作为 os.FileMode 使用）：
```go
// 创建文件 — 只传 permission bits，setuid/setgid/sticky 创建后通过 Chmod 设置
os.OpenFile(fullPath, flags, os.FileMode(msg.Mode & 0o777))

// 设置完整 mode — POSIX 数值直接传给 os.Chmod
os.Chmod(fullPath, os.FileMode(msg.Mode))
```

**关键**：不要将 POSIX 数值转换为 Go 高位表示再传给 `os.Chmod`！
- `os.Chmod(name, os.FileMode(0o4755))` ✓ — 内核收到 0o4755
- `os.Chmod(name, os.ModeSetuid|0o755)` ✗ — 内核收到 8389101（非法值）

#### 3. ListEntry 增加 UID/GID 字段

```go
type ListEntry struct {
    RelPath    string
    FileType   uint8
    FileSize   int64
    Mode       uint32  // 现在包含 setuid/setgid/sticky
    Mtime      int64
    LinkTarget string
    ETag       string
    UID        uint32  // 新增
    GID        uint32  // 新增
}
```

`infoToListEntry` 使用 `fileOwner(info)` 填充 UID/GID（需将 local 包的 `fileOwner` 提取为共享工具函数或在 ncpserver 中实现）。

#### 4. 新增 MsgChmod 和 MsgChown

当前 `SetMetadata` 只发送 `MsgUtime` 和 `MsgSetxattr`。需要扩展为发送完整元数据：

```go
// 协议新增消息类型
MsgChmod uint8 = 0x0F
MsgChown uint8 = 0x10

type ChmodMsg struct {
    Path string
    Mode uint32  // 协议级 Mode（含 setuid/setgid/sticky）
}

type ChownMsg struct {
    Path string
    UID  uint32
    GID  uint32
}
```

**客户端 `SetMetadata` 变更**：
```go
func (d *Destination) SetMetadata(ctx context.Context, relPath string, attr storage.FileAttr) error {
    // Chmod (包括 setuid/setgid/sticky)
    if attr.Mode != 0 {
        chmodMsg := &protocol.ChmodMsg{Path: fullPath, Mode: osModeToProto(attr.Mode)}
        if _, err := conn.SendMsgRecvAck(protocol.MsgChmod, chmodMsg.Encode()); err != nil { ... }
    }

    // Chown
    if attr.Uid != 0 || attr.Gid != 0 {
        chownMsg := &protocol.ChownMsg{Path: fullPath, UID: uint32(attr.Uid), GID: uint32(attr.Gid)}
        if _, err := conn.SendMsgRecvAck(protocol.MsgChown, chownMsg.Encode()); err != nil { ... }
    }

    // Utime (不变)
    ...

    // Xattr (不变)
    ...
}
```

**服务端 handler 变更**：
```go
func (h *ConnHandler) handleChmod(frame *protocol.Frame) (uint8, []byte) {
    msg := &protocol.ChmodMsg{}
    ...
    fullPath := h.fullPath(msg.Path)
    // msg.Mode 是 POSIX 数值，直接作为 os.FileMode 使用
    if err := os.Chmod(fullPath, os.FileMode(msg.Mode)); err != nil {
        return protocol.MsgError, protocol.EncodeError(model.ErrFileMetadata, err.Error())
    }
    return protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()
}

func (h *ConnHandler) handleChown(frame *protocol.Frame) (uint8, []byte) {
    msg := &protocol.ChownMsg{}
    ...
    fullPath := h.fullPath(msg.Path)
    if err := os.Chown(fullPath, int(msg.UID), int(msg.GID)); err != nil {
        // chown 失败不致命（可能无权限），但应返回错误让客户端知道
        return protocol.MsgError, protocol.EncodeError(model.ErrFileMetadata, err.Error())
    }
    return protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()
}
```

#### 5. setXattr 实现平台分发包

删除 handler.go 中的 no-op `setXattr`，改为平台特定实现（与 local 包一致）：

`internal/ncpserver/xattr_darwin.go`:
```go
//go:build darwin
package ncpserver

import "golang.org/x/sys/unix"

func setXattr(path, key, value string) error {
    return unix.Setxattr(path, key, []byte(value), 0)
}
```

`internal/ncpserver/xattr_linux.go`:
```go
//go:build linux
package ncpserver

import "golang.org/x/sys/unix"

func setXattr(path, key, value string) error {
    return unix.Setxattr(path, key, []byte(value), 0)
}
```

`internal/ncpserver/xattr_other.go`:
```go
//go:build !darwin && !linux
package ncpserver

func setXattr(path, key, value string) error {
    return fmt.Errorf("xattr not supported on this platform")
}
```

**重要**: `xattr_other.go` 返回错误而非静默成功，避免数据静默丢失。

### 版本兼容性

以上修改与 Bug #001 一起升级协议 Version 到 3。在 `ReadFrame` 中，Version != 3 的旧客户端/服务端直接拒绝连接，避免部分元数据丢失导致的静默数据损坏。

### Go os.FileMode 与 POSIX mode 的关键差异

实测验证（Go 1.23+）：

```
Go os.ModeSetuid = 0o40000000 (bit 23, dec 8388608)
Go os.ModeSetgid = 0o20000000 (bit 22, dec 4194304)
Go os.ModeSticky = 0o4000000  (bit 21, dec 1048576)

POSIX S_ISUID   = 0o4000 (bit 11, dec 2048)
POSIX S_ISGID   = 0o2000 (bit 10, dec 1024)
POSIX S_ISVTX   = 0o1000 (bit 9,  dec 512)
```

Go 的 `os.FileMode` 使用高 bit 位（bit 21-23）表示 setuid/setgid/sticky，与 POSIX（bit 9-11）**完全不兼容**。这意味着：

1. **不能**直接 `uint32(os.FileMode)` 发送到对端 — Go 的高位在 POSIX 系统上无意义
2. **不能**用 `os.ModeSetuid | 0o755` 传给 `os.Chmod` — `uint32(os.ModeSetuid|0o755) = 8389101` 对内核是非法值
3. **可以**用 `os.FileMode(0o4755)` 传给 `os.Chmod` — `uint32(FileMode(0o4755)) = 2541 = 0o4755` 对内核是合法的

**结论**：协议 Mode 字段必须使用 POSIX 数值（`0o0000`-`0o7777`），客户端发送时从 Go `FileMode` 转换，服务端接收时直接作为 `os.FileMode(ProtoMode)` 使用。

### 服务端 Mode 使用规则

```go
// 创建文件 — 只传 permission bits（0o777 掩码），setuid/setgid/sticky 在创建后通过 Chmod 设置
os.OpenFile(fullPath, flags, os.FileMode(msg.Mode & 0o777))

// 设置完整 mode（含 setuid/setgid/sticky）— 直接用 POSIX 数值
os.Chmod(fullPath, os.FileMode(msg.Mode))
```

### 客户端 local source 修复

`pkg/impls/storage/local/source.go` 的 `mode.Perm()` 应改为使用 `osModeToProto` 转换：

```go
// 修改前
Attr: storage.FileAttr{
    Mode: mode.Perm(),
    ...
}

// 修改后 — 保留 setuid/setgid/sticky
Attr: storage.FileAttr{
    Mode: mode,  // 保留完整 os.FileMode（含 Go 高位）
    ...
}
```

注意：`storage.FileAttr.Mode` 类型是 `os.FileMode`，可以承载 Go 的完整表示。关键是发送到协议层时用 `osModeToProto()` 转换为 POSIX 数值，而不是 `uint32(mode.Perm())`。

### 跨存储兼容性检查

**FS → OSS → FS**: 当前实现部分正确。OSS 通过 `ncp-mode: "0755"` 等 header 保存元数据，但只保存了 `mode.Perm()`（低 9 位），丢失了 setuid/setgid/sticky。

**修复**: OSS 的 `posixMetadata` 也应发送完整 mode:
```go
// 修改前
metaMode: fmt.Sprintf("%04o", mode.Perm()),

// 修改后
metaMode: fmt.Sprintf("%04o", osModeToProto(mode)),
```

这样 OSS header 中的 mode 将包含 setuid/setgid/sticky 位，如 `"4755"` 表示 setuid + rwxr-xr-x。

## 实施优先级

1. **P0** — OpenMsg.Flags 跨平台 (Bug #001)
2. **P0** — Mode 字段完整传递（setuid/setgid/sticky）+ osModeToProto/protoModeToOS
3. **P1** — MsgChmod / MsgChown 协议扩展
4. **P1** — setXattr 平台分发包（不再 no-op）
5. **P1** — ListEntry 增加 UID/GID
6. **P2** — OSS/COS/OBS posixMetadata 保留完整 mode
7. **P2** — local source 保留完整 mode（不再截断为 .Perm()）
