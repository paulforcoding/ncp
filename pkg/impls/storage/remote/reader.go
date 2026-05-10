package remote

import (
	"context"
	"fmt"
	"io"

	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/pkg/interfaces/storage"
)

// Reader implements storage.FileReader for remote files via the ncp protocol.
// Each Reader owns a dedicated connection and reads sequentially.
type Reader struct {
	conn   *protocol.Conn
	fd     uint32
	offset int64
}

// Read reads up to len(p) bytes from the current offset.
func (r *Reader) Read(ctx context.Context, p []byte) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	msg := &protocol.PreadMsg{
		FD:     r.fd,
		Offset: r.offset,
		Length: uint32(len(p)),
	}

	f, err := r.conn.SendAndRecv(protocol.MsgPread, msg.Encode())
	if err != nil {
		return 0, fmt.Errorf("remote pread: %w", err)
	}

	if f.Type == protocol.MsgError {
		emsg := &protocol.ErrorMsg{}
		if derr := emsg.Decode(f.Payload); derr != nil {
			return 0, fmt.Errorf("remote pread error (undecodable): %w", derr)
		}
		return 0, fmt.Errorf("remote pread error: code=0x%04X msg=%s", emsg.Code, emsg.Message)
	}

	dataMsg := &protocol.DataMsg{}
	if err := dataMsg.Decode(f.Payload); err != nil {
		return 0, fmt.Errorf("remote pread decode: %w", err)
	}

	n := copy(p, dataMsg.Data)
	if n == 0 {
		return 0, io.EOF
	}
	r.offset += int64(n)
	return n, nil
}

// Close sends MsgClose for the read fd and closes the connection.
func (r *Reader) Close(ctx context.Context) error {
	msg := &protocol.CloseMsg{FD: r.fd}
	// Best-effort close — ignore error since connection is going away
	_, _ = r.conn.SendMsgRecvAck(protocol.MsgClose, msg.Encode())
	return r.conn.Close()
}

// Size returns the file size. Remote protocol does not expose size via fd;
// callers should use the size from the DiscoverItem instead.
func (r *Reader) Size() int64 { return 0 }

// Attr returns empty attributes. Callers should use the attr from DiscoverItem.
func (r *Reader) Attr() storage.FileAttr { return storage.FileAttr{} }
