package protocol

import (
	"net"
	"sync/atomic"
	"testing"
)

func TestClientServer_RoundTrip(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var receivedType uint8

	handler := func() ConnHandler {
		return ConnHandlerFunc(func(conn *Conn) error {
			f, err := conn.Recv()
			if err != nil {
				return err
			}
			receivedType = f.Type
			return conn.Send(MsgAck, EncodeAckFD(0, 42))
		})
	}

	srv := NewServer(ln, handler)
	go srv.Serve()
	defer srv.Close()

	conn, err := Dial(ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	openMsg := &OpenMsg{Path: "/test/file.txt", Mode: 0644, UID: 1000, GID: 1000}
	ack, err := conn.SendMsgRecvAck(MsgOpen, openMsg.Encode())
	if err != nil {
		t.Fatalf("send/recv: %v", err)
	}

	if receivedType != MsgOpen {
		t.Fatalf("received type mismatch: got 0x%02X, want 0x%02X", receivedType, MsgOpen)
	}

	if ack.ResultCode != 0 {
		t.Fatalf("result code: got %d, want 0", ack.ResultCode)
	}
	_, fd := DecodeAckFD(ack.Data)
	if fd != 42 {
		t.Fatalf("fd: got %d, want 42", fd)
	}
}

func TestClientServer_MultipleMessages(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	handler := func() ConnHandler {
		return ConnHandlerFunc(func(conn *Conn) error {
			for {
				f, err := conn.Recv()
				if err != nil {
					return err
				}
				switch f.Type {
				case MsgOpen:
					msg := &OpenMsg{}
					msg.Decode(f.Payload)
					conn.Send(MsgAck, EncodeAckFD(0, 1))
				case MsgPwrite:
					conn.Send(MsgAck, EncodeAckU32(0, 4096))
				case MsgClose:
					conn.Send(MsgAck, (&AckMsg{ResultCode: 0}).Encode())
				case MsgTaskDone:
					conn.Send(MsgAck, (&AckMsg{ResultCode: 0}).Encode())
					return nil
				default:
					conn.Send(MsgError, EncodeError(0x2001, "unknown message"))
				}
			}
		})
	}

	srv := NewServer(ln, handler)
	go srv.Serve()
	defer srv.Close()

	conn, err := Dial(ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Open
	openMsg := &OpenMsg{Path: "/test/file.txt", Mode: 0644}
	ack, err := conn.SendMsgRecvAck(MsgOpen, openMsg.Encode())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_, fd := DecodeAckFD(ack.Data)
	if fd != 1 {
		t.Fatalf("fd: got %d, want 1", fd)
	}

	// Pwrite
	pwriteMsg := &PwriteMsg{FD: fd, Offset: 0, Data: make([]byte, 4096)}
	_, err = conn.SendMsgRecvAck(MsgPwrite, pwriteMsg.Encode())
	if err != nil {
		t.Fatalf("pwrite: %v", err)
	}

	// Close
	closeMsg := &CloseMsg{FD: fd, Checksum: make([]byte, 16)}
	_, err = conn.SendMsgRecvAck(MsgClose, closeMsg.Encode())
	if err != nil {
		t.Fatalf("close: %v", err)
	}

	// TaskDone
	_, err = conn.SendMsgRecvAck(MsgTaskDone, nil)
	if err != nil {
		t.Fatalf("taskdone: %v", err)
	}
}

func TestClientServer_ErrorResponse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	handler := func() ConnHandler {
		return ConnHandlerFunc(func(conn *Conn) error {
			_, err := conn.Recv()
			if err != nil {
				return err
			}
			return conn.Send(MsgError, EncodeError(0x1007, "checksum mismatch"))
		})
	}

	srv := NewServer(ln, handler)
	go srv.Serve()
	defer srv.Close()

	conn, err := Dial(ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	_, err = conn.SendMsgRecvAck(MsgClose, (&CloseMsg{FD: 1, Checksum: make([]byte, 16)}).Encode())
	if err == nil {
		t.Fatal("expected error for MsgError response")
	}
}

func TestServer_MultipleConnections(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var connCount atomic.Int32

	handler := func() ConnHandler {
		return ConnHandlerFunc(func(conn *Conn) error {
			connCount.Add(1)
			_, err := conn.Recv()
			if err != nil {
				return err
			}
			return conn.Send(MsgAck, (&AckMsg{ResultCode: 0}).Encode())
		})
	}

	srv := NewServer(ln, handler)
	go srv.Serve()
	defer srv.Close()

	for i := 0; i < 3; i++ {
		conn, err := Dial(ln.Addr().String())
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		_, err = conn.SendMsgRecvAck(MsgTaskDone, nil)
		if err != nil {
			t.Fatalf("conn %d send/recv: %v", i, err)
		}
		conn.Close()
	}

	if connCount.Load() != 3 {
		t.Fatalf("expected 3 connections, got %d", connCount.Load())
	}
}
