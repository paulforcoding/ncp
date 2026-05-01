package protocol

import (
	"fmt"
	"net"
)

// Conn wraps a net.Conn for synchronous request-response protocol messaging.
// Not safe for concurrent use — one Conn per goroutine (per Replicator).
type Conn struct {
	conn net.Conn
}

// NewConn wraps a net.Conn.
func NewConn(conn net.Conn) *Conn {
	return &Conn{conn: conn}
}

// Send writes a frame to the connection.
func (c *Conn) Send(msgType uint8, payload []byte) error {
	return WriteFrame(c.conn, msgType, payload)
}

// Recv reads a frame from the connection.
func (c *Conn) Recv() (*Frame, error) {
	return ReadFrame(c.conn)
}

// Close closes the underlying connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// RemoteAddr returns the remote address.
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SendAndRecv sends a message and waits for the response frame.
func (c *Conn) SendAndRecv(msgType uint8, payload []byte) (*Frame, error) {
	if err := c.Send(msgType, payload); err != nil {
		return nil, fmt.Errorf("send msg 0x%02X: %w", msgType, err)
	}
	f, err := c.Recv()
	if err != nil {
		return nil, fmt.Errorf("recv response for msg 0x%02X: %w", msgType, err)
	}
	return f, nil
}

// ReadAck sends a message and expects an Ack response.
// Returns result code and optional data, or an error if the response is MsgError.
func (c *Conn) SendMsgRecvAck(msgType uint8, payload []byte) (*AckMsg, error) {
	f, err := c.SendAndRecv(msgType, payload)
	if err != nil {
		return nil, err
	}

	switch f.Type {
	case MsgAck:
		ack := &AckMsg{}
		if len(f.Payload) > 0 {
			ack.Decode(f.Payload)
		}
		return ack, nil
	case MsgError:
		emsg := &ErrorMsg{}
		if len(f.Payload) > 0 {
			emsg.Decode(f.Payload)
		}
		return nil, fmt.Errorf("server error: code=0x%04X msg=%s", emsg.Code, emsg.Message)
	default:
		return nil, fmt.Errorf("unexpected response type 0x%02X", f.Type)
	}
}
