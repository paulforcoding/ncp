package remote

import (
	"fmt"

	"github.com/zp001/ncp/internal/protocol"
	pkgstorage "github.com/zp001/ncp/pkg/storage"
)

// Writer implements pkgstorage.Writer for remote files via the ncp protocol.
type Writer struct {
	conn   *protocol.Conn
	fd     uint32
	closed bool
}

var _ pkgstorage.Writer = (*Writer)(nil)

// WriteAt sends MsgPwrite to the server and returns the number of bytes written.
func (w *Writer) WriteAt(p []byte, offset int64) (int, error) {
	msg := &protocol.PwriteMsg{
		FD:     w.fd,
		Offset: offset,
		Data:   p,
	}
	ack, err := w.conn.SendMsgRecvAck(protocol.MsgPwrite, msg.Encode())
	if err != nil {
		return 0, fmt.Errorf("remote pwrite: %w", err)
	}
	_, n := protocol.DecodeAckU32(ack.Data)
	return int(n), nil
}

// Sync sends MsgFsync to the server.
func (w *Writer) Sync() error {
	msg := &protocol.FsyncMsg{FD: w.fd}
	_, err := w.conn.SendMsgRecvAck(protocol.MsgFsync, msg.Encode())
	if err != nil {
		return fmt.Errorf("remote fsync: %w", err)
	}
	return nil
}

// Close sends MsgClose with the client checksum to the server.
// The server compares its own MD5 with the client checksum.
// Does NOT close the underlying conn — the Destination owns it.
func (w *Writer) Close(checksum []byte) error {
	if w.closed {
		return nil
	}
	w.closed = true

	msg := &protocol.CloseMsg{
		FD:       w.fd,
		Checksum: checksum,
	}
	_, err := w.conn.SendMsgRecvAck(protocol.MsgClose, msg.Encode())
	if err != nil {
		return fmt.Errorf("remote close: %w", err)
	}
	return nil
}
