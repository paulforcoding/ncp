package serve

import (
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/pkg/model"
)

// ConnHandler handles all protocol messages on a single server connection.
type ConnHandler struct {
	basePath   string // set by MsgInit from client URL
	fdWriteMap map[uint32]*openWriteFile
	fdReadMap  map[uint32]*os.File
	nextFD     uint32

	// cached walk entries for MsgList (populated on first MsgList, reused for pagination)
	walkEntries []protocol.ListEntry
	walkDone    bool
}

type openWriteFile struct {
	f    *os.File
	path string
	md5  hash.Hash
}

// NewConnHandler creates a ConnHandler ready to accept a connection.
// basePath is set later via MsgInit from the client.
func NewConnHandler() *ConnHandler {
	return &ConnHandler{
		fdWriteMap: make(map[uint32]*openWriteFile),
		fdReadMap:  make(map[uint32]*os.File),
	}
}

// HandleConn implements protocol.ConnHandler.
func (h *ConnHandler) HandleConn(conn *protocol.Conn) error {
	defer h.cleanup()

	// 1. Wait for MsgInit to set basePath
	frame, err := conn.Recv()
	if err != nil {
		return err
	}
	if frame.Type != protocol.MsgInit {
		return fmt.Errorf("expected MsgInit (0x0A), got 0x%02X", frame.Type)
	}
	initMsg := &protocol.InitMsg{}
	if err := initMsg.Decode(frame.Payload); err != nil {
		_ = conn.Send(protocol.MsgError, protocol.EncodeError(model.ErrProtocol, err.Error()))
		return fmt.Errorf("decode init: %w", err)
	}
	h.basePath = initMsg.BasePath
	if err := os.MkdirAll(h.basePath, 0o755); err != nil {
		_ = conn.Send(protocol.MsgError, protocol.EncodeError(model.ErrFileMkdir, err.Error()))
		return err
	}
	if err := conn.Send(protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()); err != nil {
		return err
	}

	// 2. Message loop
	for {
		frame, err := conn.Recv()
		if err != nil {
			return err
		}

		var respType uint8
		var respPayload []byte

		switch frame.Type {
		case protocol.MsgOpen:
			respType, respPayload = h.handleOpen(frame)
		case protocol.MsgPwrite:
			respType, respPayload = h.handlePwrite(frame)
		case protocol.MsgFsync:
			respType, respPayload = h.handleFsync(frame)
		case protocol.MsgClose:
			respType, respPayload = h.handleClose(frame)
		case protocol.MsgAbortFile:
			respType, respPayload = h.handleAbortFile(frame)
		case protocol.MsgMkdir:
			respType, respPayload = h.handleMkdir(frame)
		case protocol.MsgSymlink:
			respType, respPayload = h.handleSymlink(frame)
		case protocol.MsgUtime:
			respType, respPayload = h.handleUtime(frame)
		case protocol.MsgSetxattr:
			respType, respPayload = h.handleSetxattr(frame)
		case protocol.MsgTaskDone:
			respType = protocol.MsgAck
			respPayload = (&protocol.AckMsg{ResultCode: 0}).Encode()
			if err := conn.Send(respType, respPayload); err != nil {
				return err
			}
			return nil
		case protocol.MsgList:
			respType, respPayload = h.handleList(frame)
		case protocol.MsgPread:
			respType, respPayload = h.handlePread(frame)
		case protocol.MsgStat:
			respType, respPayload = h.handleStat(frame)
		default:
			respType = protocol.MsgError
			respPayload = protocol.EncodeError(model.ErrProtocol, "unknown message type")
		}

		if err := conn.Send(respType, respPayload); err != nil {
			return err
		}
	}
}

// fullPath joins basePath with relPath to produce an absolute path.
func (h *ConnHandler) fullPath(relPath string) string {
	return filepath.Join(h.basePath, relPath)
}

// --- Write operations (destination) ---

func (h *ConnHandler) handleOpen(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.OpenMsg{}
	if err := msg.Decode(frame.Payload); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrProtocol, err.Error())
	}

	if msg.Flags == 0 { // O_RDONLY
		return h.handleOpenRead(msg)
	}

	// Write mode
	fullPath := h.fullPath(msg.Path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileMkdir, err.Error())
	}

	f, err := os.OpenFile(fullPath, int(msg.Flags), os.FileMode(msg.Mode))
	if err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileOpen, err.Error())
	}

	fd := h.nextFD
	h.nextFD++
	h.fdWriteMap[fd] = &openWriteFile{f: f, path: fullPath, md5: md5.New()}

	return protocol.MsgAck, protocol.EncodeAckFD(0, fd)
}

func (h *ConnHandler) handleOpenRead(msg *protocol.OpenMsg) (uint8, []byte) {
	fullPath := h.fullPath(msg.Path)
	f, err := os.Open(fullPath)
	if err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileOpen, err.Error())
	}

	fd := h.nextFD
	h.nextFD++
	h.fdReadMap[fd] = f

	return protocol.MsgAck, protocol.EncodeAckFD(0, fd)
}

func (h *ConnHandler) handlePwrite(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.PwriteMsg{}
	if err := msg.Decode(frame.Payload); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrProtocol, err.Error())
	}

	of, ok := h.fdWriteMap[msg.FD]
	if !ok {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileWrite, "bad fd")
	}

	n, err := of.f.WriteAt(msg.Data, msg.Offset)
	if err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileWrite, err.Error())
	}

	of.md5.Write(msg.Data)

	return protocol.MsgAck, protocol.EncodeAckU32(0, uint32(n))
}

func (h *ConnHandler) handleFsync(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.FsyncMsg{}
	if err := msg.Decode(frame.Payload); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrProtocol, err.Error())
	}

	of, ok := h.fdWriteMap[msg.FD]
	if !ok {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileWrite, "bad fd")
	}

	if err := of.f.Sync(); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileWrite, err.Error())
	}

	return protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()
}

func (h *ConnHandler) handleClose(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.CloseMsg{}
	if err := msg.Decode(frame.Payload); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrProtocol, err.Error())
	}

	if of, ok := h.fdWriteMap[msg.FD]; ok {
		serverMD5 := of.md5.Sum(nil)
		of.f.Close()
		delete(h.fdWriteMap, msg.FD)

		if !equalChecksum(msg.Checksum, serverMD5) {
			return protocol.MsgError, protocol.EncodeError(model.ErrChecksumMismatch,
				fmt.Sprintf("checksum mismatch: client=%x server=%x", msg.Checksum, serverMD5))
		}
		return protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()
	}

	if f, ok := h.fdReadMap[msg.FD]; ok {
		f.Close()
		delete(h.fdReadMap, msg.FD)
		return protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()
	}

	return protocol.MsgError, protocol.EncodeError(model.ErrFileOpen, "bad fd")
}

func (h *ConnHandler) handleAbortFile(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.AbortFileMsg{}
	if err := msg.Decode(frame.Payload); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrProtocol, err.Error())
	}

	if of, ok := h.fdWriteMap[msg.FD]; ok {
		of.f.Close()
		_ = os.Remove(of.path)
		delete(h.fdWriteMap, msg.FD)
		return protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()
	}

	return protocol.MsgError, protocol.EncodeError(model.ErrFileOpen, "bad fd")
}

func (h *ConnHandler) handleMkdir(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.MkdirMsg{}
	if err := msg.Decode(frame.Payload); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrProtocol, err.Error())
	}

	fullPath := h.fullPath(msg.Path)
	if err := os.MkdirAll(fullPath, os.FileMode(msg.Mode)); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileMkdir, err.Error())
	}

	return protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()
}

func (h *ConnHandler) handleSymlink(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.SymlinkMsg{}
	if err := msg.Decode(frame.Payload); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrProtocol, err.Error())
	}

	linkPath := h.fullPath(msg.LinkPath)
	if err := os.MkdirAll(filepath.Dir(linkPath), 0o755); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileMkdir, err.Error())
	}
	_ = os.Remove(linkPath)
	if err := os.Symlink(msg.Target, linkPath); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileSymlink, err.Error())
	}

	return protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()
}

func (h *ConnHandler) handleUtime(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.UtimeMsg{}
	if err := msg.Decode(frame.Payload); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrProtocol, err.Error())
	}

	fullPath := h.fullPath(msg.Path)
	if err := os.Chtimes(fullPath, time.Unix(0, msg.Atime), time.Unix(0, msg.Mtime)); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileMetadata, err.Error())
	}

	return protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()
}

func (h *ConnHandler) handleSetxattr(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.SetxattrMsg{}
	if err := msg.Decode(frame.Payload); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrProtocol, err.Error())
	}

	fullPath := h.fullPath(msg.Path)
	if err := setXattr(fullPath, msg.Key, msg.Value); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileMetadata, err.Error())
	}

	return protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()
}

// --- Read operations (source) ---

const listPageSize = 1000

func (h *ConnHandler) handleList(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.ListMsg{}
	if err := msg.Decode(frame.Payload); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrProtocol, err.Error())
	}

	// Populate walk entries on first request (token == "")
	if msg.ContinuationToken == "" && !h.walkDone {
		entries, err := h.walkDir()
		if err != nil {
			return protocol.MsgError, protocol.EncodeError(model.ErrFileRead, err.Error())
		}
		h.walkEntries = entries
		h.walkDone = true
	}

	// Parse continuation token as offset
	offset := 0
	if msg.ContinuationToken != "" {
		if _, err := fmt.Sscanf(msg.ContinuationToken, "%d", &offset); err != nil {
			return protocol.MsgError, protocol.EncodeError(model.ErrProtocol,
				fmt.Sprintf("bad continuation token %q: %v", msg.ContinuationToken, err))
		}
	}

	end := offset + listPageSize
	if end > len(h.walkEntries) {
		end = len(h.walkEntries)
	}

	page := h.walkEntries[offset:end]

	var nextToken string
	if end < len(h.walkEntries) {
		nextToken = fmt.Sprintf("%d", end)
	}

	resp := &protocol.DataMsg{
		ResultCode:        0,
		Entries:           page,
		ContinuationToken: nextToken,
	}

	return protocol.MsgData, resp.Encode()
}

func (h *ConnHandler) handlePread(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.PreadMsg{}
	if err := msg.Decode(frame.Payload); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrProtocol, err.Error())
	}

	f, ok := h.fdReadMap[msg.FD]
	if !ok {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileRead, "bad fd")
	}

	buf := make([]byte, msg.Length)
	n, err := f.ReadAt(buf, msg.Offset)
	if err != nil && n == 0 && err != io.EOF {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileRead, err.Error())
	}

	resp := &protocol.DataMsg{
		ResultCode: 0,
		Data:       buf[:n],
	}

	return protocol.MsgData, resp.Encode()
}

func (h *ConnHandler) handleStat(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.StatMsg{}
	if err := msg.Decode(frame.Payload); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrProtocol, err.Error())
	}

	fullPath := h.fullPath(msg.RelPath)
	info, err := os.Lstat(fullPath)
	if err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileOpen, err.Error())
	}

	entry := infoToListEntry(msg.RelPath, info, fullPath)

	resp := &protocol.DataMsg{
		ResultCode: 0,
		Entries:    []protocol.ListEntry{entry},
	}

	return protocol.MsgData, resp.Encode()
}

// walkDir traverses basePath and returns all ListEntry items.
func (h *ConnHandler) walkDir() ([]protocol.ListEntry, error) {
	var entries []protocol.ListEntry

	err := filepath.Walk(h.basePath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		relPath, err := filepath.Rel(h.basePath, path)
		if err != nil {
			return nil
		}
		relPath = filepath.ToSlash(relPath)
		if relPath == "." {
			return nil
		}

		// Skip special file types
		mode := info.Mode()
		if mode&fs.ModeDevice != 0 || mode&fs.ModeNamedPipe != 0 || mode&fs.ModeSocket != 0 {
			return nil
		}

		entry := infoToListEntry(relPath, info, path)
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].RelPath < entries[j].RelPath
	})

	return entries, nil
}

// infoToListEntry converts os.FileInfo to a protocol ListEntry.
// fullPath is used to read symlink targets.
func infoToListEntry(relPath string, info fs.FileInfo, fullPath string) protocol.ListEntry {
	mode := info.Mode()
	var ft uint8
	switch {
	case info.IsDir():
		ft = uint8(model.FileDir)
	case mode&fs.ModeSymlink != 0:
		ft = uint8(model.FileSymlink)
	default:
		ft = uint8(model.FileRegular)
	}

	entry := protocol.ListEntry{
		RelPath:  relPath,
		FileType: ft,
		FileSize: info.Size(),
		Mode:     uint32(mode.Perm()),
		Mtime:    info.ModTime().UnixNano(),
	}

	if mode&fs.ModeSymlink != 0 {
		if target, err := os.Readlink(fullPath); err == nil {
			entry.LinkTarget = target
		}
	}

	return entry
}

func (h *ConnHandler) cleanup() {
	for fd, of := range h.fdWriteMap {
		of.f.Close()
		delete(h.fdWriteMap, fd)
	}
	for fd, f := range h.fdReadMap {
		f.Close()
		delete(h.fdReadMap, fd)
	}
}

func equalChecksum(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// setXattr sets an extended attribute on a file.
// This is a no-op on platforms that don't support xattr.
func setXattr(path, key, value string) error {
	return nil
}
