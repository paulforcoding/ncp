package remote

import (
	"context"
	"fmt"

	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/pkg/interfaces/storage"
)

// Writer implements storage.FileWriter for remote files via the ncp protocol.
type Writer struct {
	conn         *protocol.Conn
	fd           uint32
	bytesWritten int64
	committed    bool
	aborted      bool
}

var _ storage.FileWriter = (*Writer)(nil)

// Write sends MsgPwrite to the server and returns the number of bytes written.
func (w *Writer) Write(_ context.Context, p []byte) (int, error) {
	if w.committed || w.aborted {
		return 0, fmt.Errorf("remote: write on closed writer")
	}

	msg := &protocol.PwriteMsg{
		FD:     w.fd,
		Offset: w.bytesWritten,
		Data:   p,
	}
	ack, err := w.conn.SendMsgRecvAck(protocol.MsgPwrite, msg.Encode())
	if err != nil {
		return 0, fmt.Errorf("remote pwrite: %w", err)
	}
	_, n := protocol.DecodeAckU32(ack.Data)
	w.bytesWritten += int64(n)
	return int(n), nil
}

// Commit sends MsgClose with the client checksum to the server.
func (w *Writer) Commit(_ context.Context, checksum []byte) error {
	if w.committed || w.aborted {
		return nil
	}
	w.committed = true

	msg := &protocol.CloseMsg{
		FD:       w.fd,
		Checksum: checksum,
	}
	_, err := w.conn.SendMsgRecvAck(protocol.MsgClose, msg.Encode())
	if err != nil {
		return fmt.Errorf("remote commit: %w", err)
	}
	return nil
}

// Abort sends MsgAbortFile to the server.
func (w *Writer) Abort(_ context.Context) error {
	if w.committed || w.aborted {
		return nil
	}
	w.aborted = true

	msg := &protocol.AbortFileMsg{FD: w.fd}
	_, err := w.conn.SendMsgRecvAck(protocol.MsgAbortFile, msg.Encode())
	if err != nil {
		return fmt.Errorf("remote abort: %w", err)
	}
	return nil
}

// BytesWritten returns the number of bytes written so far.
func (w *Writer) BytesWritten() int64 { return w.bytesWritten }
