package remote

import (
	"fmt"
	"io"

	"github.com/zp001/ncp/internal/protocol"
)

// Reader implements storage.Reader for remote files via the ncp protocol.
// Each Reader owns a dedicated connection.
type Reader struct {
	conn *protocol.Conn
	fd   uint32
}

// ReadAt sends MsgPread to the server and copies data into p.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	msg := &protocol.PreadMsg{
		FD:     r.fd,
		Offset: off,
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
	return n, nil
}

// Close sends MsgClose for the read fd and closes the connection.
func (r *Reader) Close() error {
	msg := &protocol.CloseMsg{FD: r.fd}
	// Best-effort close — ignore error since connection is going away
	_, _ = r.conn.SendMsgRecvAck(protocol.MsgClose, msg.Encode())
	return r.conn.Close()
}
