package main

import (
	"crypto/md5"
	"fmt"
	"hash"
	"os"

	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// serveConnHandler handles all protocol messages on a single server connection.
type serveConnHandler struct {
	dst    storage.Destination
	fdMap  map[uint32]*openFile
	nextFD uint32
}

type openFile struct {
	writer storage.Writer
	md5    hash.Hash
}

func newServeConnHandler(dst storage.Destination) *serveConnHandler {
	return &serveConnHandler{
		dst:   dst,
		fdMap: make(map[uint32]*openFile),
	}
}

func (h *serveConnHandler) HandleConn(conn *protocol.Conn) error {
	defer h.cleanup()
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
			conn.Send(respType, respPayload)
			return nil
		default:
			respType = protocol.MsgError
			respPayload = protocol.EncodeError(model.ErrProtocol, "unknown message type")
		}

		if err := conn.Send(respType, respPayload); err != nil {
			return err
		}
	}
}

func (h *serveConnHandler) handleOpen(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.OpenMsg{}
	msg.Decode(frame.Payload)

	writer, err := h.dst.OpenFile(msg.Path, 0, os.FileMode(msg.Mode), int(msg.UID), int(msg.GID))
	if err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileOpen, err.Error())
	}

	fd := h.nextFD
	h.nextFD++
	h.fdMap[fd] = &openFile{writer: writer, md5: md5.New()}

	return protocol.MsgAck, protocol.EncodeAckFD(0, fd)
}

func (h *serveConnHandler) handlePwrite(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.PwriteMsg{}
	msg.Decode(frame.Payload)

	of, ok := h.fdMap[msg.FD]
	if !ok {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileWrite, "bad fd")
	}

	n, err := of.writer.WriteAt(msg.Data, msg.Offset)
	if err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileWrite, err.Error())
	}

	of.md5.Write(msg.Data)

	return protocol.MsgAck, protocol.EncodeAckU32(0, uint32(n))
}

func (h *serveConnHandler) handleFsync(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.FsyncMsg{}
	msg.Decode(frame.Payload)

	of, ok := h.fdMap[msg.FD]
	if !ok {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileWrite, "bad fd")
	}

	if err := of.writer.Sync(); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileWrite, err.Error())
	}

	return protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()
}

func (h *serveConnHandler) handleClose(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.CloseMsg{}
	msg.Decode(frame.Payload)

	of, ok := h.fdMap[msg.FD]
	if !ok {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileOpen, "bad fd")
	}

	serverMD5 := of.md5.Sum(nil)
	err := of.writer.Close(nil) // local writer ignores checksum
	if err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileWrite, err.Error())
	}

	if !equalChecksum(msg.Checksum, serverMD5) {
		delete(h.fdMap, msg.FD)
		return protocol.MsgError, protocol.EncodeError(model.ErrChecksumMismatch,
			fmt.Sprintf("checksum mismatch: client=%x server=%x", msg.Checksum, serverMD5))
	}

	delete(h.fdMap, msg.FD)
	return protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()
}

func (h *serveConnHandler) handleMkdir(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.MkdirMsg{}
	msg.Decode(frame.Payload)

	if err := h.dst.Mkdir(msg.Path, os.FileMode(msg.Mode), int(msg.UID), int(msg.GID)); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileMkdir, err.Error())
	}

	return protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()
}

func (h *serveConnHandler) handleSymlink(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.SymlinkMsg{}
	msg.Decode(frame.Payload)

	if err := h.dst.Symlink(msg.LinkPath, msg.Target); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileSymlink, err.Error())
	}

	return protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()
}

func (h *serveConnHandler) handleUtime(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.UtimeMsg{}
	msg.Decode(frame.Payload)

	meta := model.FileMetadata{
		Atime: msg.Atime,
		Mtime: msg.Mtime,
	}
	if err := h.dst.SetMetadata(msg.Path, meta); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileMetadata, err.Error())
	}

	return protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()
}

func (h *serveConnHandler) handleSetxattr(frame *protocol.Frame) (uint8, []byte) {
	msg := &protocol.SetxattrMsg{}
	msg.Decode(frame.Payload)

	meta := model.FileMetadata{
		Xattr: map[string]string{msg.Key: msg.Value},
	}
	if err := h.dst.SetMetadata(msg.Path, meta); err != nil {
		return protocol.MsgError, protocol.EncodeError(model.ErrFileMetadata, err.Error())
	}

	return protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode()
}

func (h *serveConnHandler) cleanup() {
	for fd, of := range h.fdMap {
		of.writer.Close(nil)
		delete(h.fdMap, fd)
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
