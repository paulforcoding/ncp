package ncpserver

import (
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/pkg/model"
)

// ConnHandler handles all protocol messages on a single server connection.
type ConnHandler struct {
	server     *Server
	basePath   string
	mode       uint8
	taskID     string
	walker     *taskWalker // Source mode only; references server.walker
	fdWriteMap map[uint32]*openWriteFile
	fdReadMap  map[uint32]*os.File
	nextFD     uint32
}

// protoFlagsToOS converts protocol-level ProtoO_* flags to OS-specific os.O_* flags.
func protoFlagsToOS(pf uint32) int {
	var flags int
	if pf&protocol.ProtoO_WRONLY != 0 {
		flags |= os.O_WRONLY
	}
	if pf&protocol.ProtoO_RDWR != 0 {
		flags |= os.O_RDWR
	}
	if pf&protocol.ProtoO_CREAT != 0 {
		flags |= os.O_CREATE
	}
	if pf&protocol.ProtoO_TRUNC != 0 {
		flags |= os.O_TRUNC
	}
	if pf&protocol.ProtoO_APPEND != 0 {
		flags |= os.O_APPEND
	}
	return flags
}

// osModeToProto converts Go os.FileMode to POSIX permission bits for the protocol.
func osModeToProto(mode os.FileMode) uint32 {
	pm := uint32(mode.Perm())
	if mode&os.ModeSetuid != 0 {
		pm |= protocol.ProtoModeSetuid
	}
	if mode&os.ModeSetgid != 0 {
		pm |= protocol.ProtoModeSetgid
	}
	if mode&os.ModeSticky != 0 {
		pm |= protocol.ProtoModeSticky
	}
	return pm
}

type openWriteFile struct {
	f    *os.File
	path string
	md5  hash.Hash
}

// NewConnHandler creates a ConnHandler for the given server.
func NewConnHandler(server *Server) *ConnHandler {
	return &ConnHandler{
		server:     server,
		fdWriteMap: make(map[uint32]*openWriteFile),
		fdReadMap:  make(map[uint32]*os.File),
	}
}

// HandleConn implements the ncp protocol message loop.
func (h *ConnHandler) HandleConn(conn *protocol.Conn) error {
	defer h.cleanup()

	// 1. Wait for MsgInit
	frame, err := conn.Recv()
	if err != nil {
		return err
	}
	if frame.Type != protocol.MsgInit {
		return fmt.Errorf("expected MsgInit, got 0x%02X", frame.Type)
	}
	initMsg := &protocol.InitMsg{}
	if err := initMsg.Decode(frame.Payload); err != nil {
		_ = conn.Send(protocol.MsgError, protocol.EncodeError(model.ErrProtocol, err.Error()))
		return err
	}

	// Reject old clients that don't send Mode and TaskID
	if initMsg.Mode == 0 || initMsg.TaskID == "" {
		_ = conn.Send(protocol.MsgError, protocol.EncodeError(model.ErrProtocol, "client must send Mode and TaskID"))
		return fmt.Errorf("old client rejected: missing Mode/TaskID")
	}

	// Server state machine validation
	h.server.mu.Lock()
	if h.server.mode == ServerModeUninitialized {
		// First connection: create walker for Source, skip for SourceNoWalker, MkdirAll for Destination
		if initMsg.Mode == protocol.ModeSource {
			h.server.walker = newTaskWalker(initMsg.TaskID, initMsg.BasePath, h.server.tempDir)
			h.server.walker.start()
		} else if initMsg.Mode == protocol.ModeSourceNoWalker {
			// SourceNoWalker: no walker needed
		} else {
			_ = os.MkdirAll(initMsg.BasePath, 0o755)
		}
		h.server.mode = initMsg.Mode
		h.server.taskID = initMsg.TaskID
	} else {
		if initMsg.Mode != h.server.mode {
			h.server.mu.Unlock()
			_ = conn.Send(protocol.MsgError, protocol.EncodeError(model.ErrProtocol,
				fmt.Sprintf("server already in mode %d", h.server.mode)))
			return fmt.Errorf("mode mismatch")
		}
		if initMsg.TaskID != h.server.taskID {
			h.server.mu.Unlock()
			_ = conn.Send(protocol.MsgError, protocol.EncodeError(model.ErrProtocol,
				fmt.Sprintf("server serving task %q, restart to switch", h.server.taskID)))
			return fmt.Errorf("taskID mismatch")
		}
	}
	h.server.mu.Unlock()

	h.basePath = initMsg.BasePath
	h.mode = initMsg.Mode
	h.taskID = initMsg.TaskID
	if h.mode == protocol.ModeSource {
		h.walker = h.server.walker
	}

	_ = conn.Send(protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode())

	// 2. Message loop
	for {
		frame, err := conn.Recv()
		if err != nil {
			return err
		}

		var respType uint8
		var respPayload []byte

		switch frame.Type {
		case protocol.MsgTaskDone:
			respType = protocol.MsgAck
			respPayload = (&protocol.AckMsg{ResultCode: 0}).Encode()
			_ = conn.Send(respType, respPayload)
			h.server.Shutdown()
			return nil

		case protocol.MsgList:
			if h.mode != protocol.ModeSource {
				respType = protocol.MsgError
				respPayload = protocol.EncodeError(model.ErrProtocol, "MsgList only allowed in Source mode")
				break
			}
			respType, respPayload = h.handleList(frame)

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
		case protocol.MsgChmod:
			respType, respPayload = h.handleChmod(frame)
		case protocol.MsgChown:
			respType, respPayload = h.handleChown(frame)
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

// fullPath joins basePath with relPath.
func (h *ConnHandler) fullPath(relPath string) string {
	return filepath.Join(h.basePath, relPath)
}

// --- Write operations (destination) ---

func (h *ConnHandler) handleOpen(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.OpenMsg{}
	if err := msg.Decode(frame.Payload); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrProtocol, err.Error())
	}

	if msg.Flags == 0 { // ProtoO_RDONLY
		return h.handleOpenRead(msg)
	}

	fullPath := h.fullPath(msg.Path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileMkdir, err.Error())
	}

	f, err := os.OpenFile(fullPath, protoFlagsToOS(msg.Flags), os.FileMode(msg.Mode&0o777))
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
	if err := os.MkdirAll(fullPath, os.FileMode(msg.Mode&0o777)); err != nil {
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

func (h *ConnHandler) handleChmod(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.ChmodMsg{}
	if err := msg.Decode(frame.Payload); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrProtocol, err.Error())
	}

	fullPath := h.fullPath(msg.Path)
	if err := os.Chmod(fullPath, os.FileMode(msg.Mode)); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileMetadata, err.Error())
	}

	return protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()
}

func (h *ConnHandler) handleChown(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.ChownMsg{}
	if err := msg.Decode(frame.Payload); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrProtocol, err.Error())
	}

	fullPath := h.fullPath(msg.Path)
	if err := os.Chown(fullPath, int(msg.UID), int(msg.GID)); err != nil {
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

	// Parse continuation token as sequence number
	seq := int64(0)
	if msg.ContinuationToken != "" {
		if _, err := fmt.Sscanf(msg.ContinuationToken, "%d", &seq); err != nil {
			return protocol.MsgError, protocol.EncodeError(model.ErrProtocol,
				fmt.Sprintf("bad continuation token %q: %v", msg.ContinuationToken, err))
		}
	}

	entries, done, err := h.walker.waitForEntries(seq, listPageSize)
	if err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileRead, err.Error())
	}

	page := make([]protocol.ListEntry, len(entries))
	for i, e := range entries {
		page[i] = e.Entry
	}

	var nextToken string
	if !done || len(entries) == listPageSize {
		// There may be more entries; use next seq as token
		nextSeq := seq + int64(len(entries))
		nextToken = fmt.Sprintf("%d", nextSeq)
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
