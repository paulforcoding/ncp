package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestWriteReadFrame_EmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, MsgAck, nil); err != nil {
		t.Fatalf("write: %v", err)
	}

	f, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if f.Type != MsgAck {
		t.Fatalf("type mismatch: got %d, want %d", f.Type, MsgAck)
	}
	if f.Version != Version {
		t.Fatalf("version mismatch: got %d, want %d", f.Version, Version)
	}
	if len(f.Payload) != 0 {
		t.Fatalf("payload should be empty, got %d bytes", len(f.Payload))
	}
}

func TestWriteReadFrame_WithPayload(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte("hello world")
	if err := WriteFrame(&buf, MsgOpen, payload); err != nil {
		t.Fatalf("write: %v", err)
	}

	f, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if f.Type != MsgOpen {
		t.Fatalf("type mismatch: got %d", f.Type)
	}
	if !bytes.Equal(f.Payload, payload) {
		t.Fatalf("payload mismatch: got %q, want %q", f.Payload, payload)
	}
}

func TestWriteReadFrame_LargePayload(t *testing.T) {
	var buf bytes.Buffer
	payload := make([]byte, 4*1024*1024) // 4 MB
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	if err := WriteFrame(&buf, MsgPwrite, payload); err != nil {
		t.Fatalf("write: %v", err)
	}

	f, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(f.Payload, payload) {
		t.Fatalf("large payload mismatch")
	}
}

func TestWriteFrame_TooLarge(t *testing.T) {
	var buf bytes.Buffer
	payload := make([]byte, MaxPayloadSize+1)
	if err := WriteFrame(&buf, MsgPwrite, payload); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("expected ErrTooLarge, got %v", err)
	}
}

func TestReadFrame_BadMagic(t *testing.T) {
	var buf bytes.Buffer
	var hdr [HeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], 0xDEADBEEF)
	hdr[4] = Version
	hdr[5] = MsgAck
	binary.BigEndian.PutUint32(hdr[6:10], 0)
	buf.Write(hdr[:])

	_, err := ReadFrame(&buf)
	if !errors.Is(err, ErrBadMagic) {
		t.Fatalf("expected ErrBadMagic, got %v", err)
	}
}

func TestReadFrame_BadVersion(t *testing.T) {
	var buf bytes.Buffer
	var hdr [HeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], Magic)
	hdr[4] = 99
	hdr[5] = MsgAck
	binary.BigEndian.PutUint32(hdr[6:10], 0)
	buf.Write(hdr[:])

	_, err := ReadFrame(&buf)
	if !errors.Is(err, ErrBadVersion) {
		t.Fatalf("expected ErrBadVersion, got %v", err)
	}
}

func TestReadFrame_TruncatedHeader(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x4E, 0x43, 0x50})
	_, err := ReadFrame(buf)
	if err == nil {
		t.Fatal("expected error for truncated header")
	}
}

func TestReadFrame_TruncatedPayload(t *testing.T) {
	var buf bytes.Buffer
	var hdr [HeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], Magic)
	hdr[4] = Version
	hdr[5] = MsgPwrite
	binary.BigEndian.PutUint32(hdr[6:10], 100)
	buf.Write(hdr[:])
	buf.Write([]byte("short"))

	_, err := ReadFrame(&buf)
	if err == nil {
		t.Fatal("expected error for truncated payload")
	}
}

func TestRoundTrip_AllMessageTypes(t *testing.T) {
	types := []uint8{
		MsgOpen, MsgPwrite, MsgFsync, MsgClose,
		MsgMkdir, MsgSymlink, MsgUtime, MsgSetxattr,
		MsgTaskDone, MsgAck, MsgError,
	}
	for _, mt := range types {
		var buf bytes.Buffer
		if err := WriteFrame(&buf, mt, []byte{1, 2, 3}); err != nil {
			t.Fatalf("write type %d: %v", mt, err)
		}
		f, err := ReadFrame(&buf)
		if err != nil {
			t.Fatalf("read type %d: %v", mt, err)
		}
		if f.Type != mt {
			t.Fatalf("type %d: got %d", mt, f.Type)
		}
	}
}
