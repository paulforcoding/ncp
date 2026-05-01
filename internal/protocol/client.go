package protocol

import (
	"fmt"
	"net"
)

// Dial establishes a TCP connection to an ncp server and returns a Conn.
func Dial(addr string) (*Conn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("protocol dial %s: %w", addr, err)
	}
	return NewConn(conn), nil
}
