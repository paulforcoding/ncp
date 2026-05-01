package protocol

import (
	"bytes"
	"testing"
)

func TestOpenMsg_Roundtrip(t *testing.T) {
	m := &OpenMsg{
		Path:  "/data/dir/file.txt",
		Flags: 0x241, // O_WRONLY | O_CREATE | O_TRUNC
		Mode:  0644,
		UID:   1000,
		GID:   1000,
	}
	data := m.Encode()

	m2 := &OpenMsg{}
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m2.Path != m.Path {
		t.Fatalf("path mismatch: got %q, want %q", m2.Path, m.Path)
	}
	if m2.Flags != m.Flags {
		t.Fatalf("flags mismatch: got %d, want %d", m2.Flags, m.Flags)
	}
	if m2.Mode != m.Mode {
		t.Fatalf("mode mismatch: got %d, want %d", m2.Mode, m.Mode)
	}
	if m2.UID != m.UID {
		t.Fatalf("uid mismatch: got %d, want %d", m2.UID, m.UID)
	}
	if m2.GID != m.GID {
		t.Fatalf("gid mismatch: got %d, want %d", m2.GID, m.GID)
	}
}

func TestPwriteMsg_Roundtrip(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	m := &PwriteMsg{
		FD:     3,
		Offset: 4096,
		Data:   data,
	}
	encoded := m.Encode()

	m2 := &PwriteMsg{}
	if err := m2.Decode(encoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m2.FD != m.FD {
		t.Fatalf("fd mismatch: got %d, want %d", m2.FD, m.FD)
	}
	if m2.Offset != m.Offset {
		t.Fatalf("offset mismatch: got %d, want %d", m2.Offset, m.Offset)
	}
	if !bytes.Equal(m2.Data, m.Data) {
		t.Fatalf("data mismatch")
	}
}

func TestPwriteMsg_LargeData(t *testing.T) {
	data := make([]byte, 4*1024*1024) // 4 MB
	for i := range data {
		data[i] = byte(i % 256)
	}
	m := &PwriteMsg{FD: 1, Offset: 0, Data: data}
	encoded := m.Encode()

	m2 := &PwriteMsg{}
	if err := m2.Decode(encoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(m2.Data, data) {
		t.Fatalf("large data mismatch")
	}
}

func TestFsyncMsg_Roundtrip(t *testing.T) {
	m := &FsyncMsg{FD: 5}
	data := m.Encode()
	m2 := &FsyncMsg{}
	m2.Decode(data)
	if m2.FD != m.FD {
		t.Fatalf("fd mismatch: got %d, want %d", m2.FD, m.FD)
	}
}

func TestCloseMsg_Roundtrip(t *testing.T) {
	checksum := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	m := &CloseMsg{FD: 3, Checksum: checksum}
	data := m.Encode()

	m2 := &CloseMsg{}
	m2.Decode(data)
	if m2.FD != m.FD {
		t.Fatalf("fd mismatch: got %d, want %d", m2.FD, m.FD)
	}
	if !bytes.Equal(m2.Checksum, checksum) {
		t.Fatalf("checksum mismatch")
	}
}

func TestMkdirMsg_Roundtrip(t *testing.T) {
	m := &MkdirMsg{Path: "/data/dir", Mode: 0755, UID: 0, GID: 0}
	data := m.Encode()

	m2 := &MkdirMsg{}
	m2.Decode(data)
	if m2.Path != m.Path {
		t.Fatalf("path mismatch: got %q, want %q", m2.Path, m.Path)
	}
	if m2.Mode != m.Mode {
		t.Fatalf("mode mismatch")
	}
}

func TestSymlinkMsg_Roundtrip(t *testing.T) {
	m := &SymlinkMsg{Target: "/data/original", LinkPath: "/data/link"}
	data := m.Encode()

	m2 := &SymlinkMsg{}
	m2.Decode(data)
	if m2.Target != m.Target {
		t.Fatalf("target mismatch: got %q, want %q", m2.Target, m.Target)
	}
	if m2.LinkPath != m.LinkPath {
		t.Fatalf("linkpath mismatch: got %q, want %q", m2.LinkPath, m.LinkPath)
	}
}

func TestUtimeMsg_Roundtrip(t *testing.T) {
	m := &UtimeMsg{Path: "/data/file.txt", Atime: 1700000000000000000, Mtime: 1700000001000000000}
	data := m.Encode()

	m2 := &UtimeMsg{}
	m2.Decode(data)
	if m2.Path != m.Path {
		t.Fatalf("path mismatch")
	}
	if m2.Atime != m.Atime {
		t.Fatalf("atime mismatch: got %d, want %d", m2.Atime, m.Atime)
	}
	if m2.Mtime != m.Mtime {
		t.Fatalf("mtime mismatch: got %d, want %d", m2.Mtime, m.Mtime)
	}
}

func TestSetxattrMsg_Roundtrip(t *testing.T) {
	m := &SetxattrMsg{Path: "/data/file.txt", Key: "user.comment", Value: "hello"}
	data := m.Encode()

	m2 := &SetxattrMsg{}
	m2.Decode(data)
	if m2.Path != m.Path {
		t.Fatalf("path mismatch")
	}
	if m2.Key != m.Key {
		t.Fatalf("key mismatch: got %q, want %q", m2.Key, m.Key)
	}
	if m2.Value != m.Value {
		t.Fatalf("value mismatch: got %q, want %q", m2.Value, m.Value)
	}
}

func TestTaskDoneMsg_Roundtrip(t *testing.T) {
	m := &TaskDoneMsg{}
	data := m.Encode()
	if len(data) != 0 {
		t.Fatalf("expected empty payload, got %d bytes", len(data))
	}
	m2 := &TaskDoneMsg{}
	m2.Decode(data)
}

func TestAckMsg_Roundtrip(t *testing.T) {
	m := &AckMsg{ResultCode: 0, Data: []byte{0, 0, 0, 42}}
	data := m.Encode()

	m2 := &AckMsg{}
	m2.Decode(data)
	if m2.ResultCode != m.ResultCode {
		t.Fatalf("resultcode mismatch: got %d, want %d", m2.ResultCode, m.ResultCode)
	}
	if !bytes.Equal(m2.Data, m.Data) {
		t.Fatalf("data mismatch")
	}
}

func TestErrorMsg_Roundtrip(t *testing.T) {
	m := &ErrorMsg{Code: 0x1007, Message: "checksum mismatch"}
	data := m.Encode()

	m2 := &ErrorMsg{}
	m2.Decode(data)
	if m2.Code != m.Code {
		t.Fatalf("code mismatch: got %d, want %d", m2.Code, m.Code)
	}
	if m2.Message != m.Message {
		t.Fatalf("message mismatch: got %q, want %q", m2.Message, m.Message)
	}
}

func TestEncodeAckFD(t *testing.T) {
	data := EncodeAckFD(0, 42)
	// Decode the full AckMsg first, then extract fd from Data
	ack := &AckMsg{}
	ack.Decode(data)
	_, fd := DecodeAckFD(ack.Data)
	if fd != 42 {
		t.Fatalf("fd mismatch: got %d, want 42", fd)
	}
}

func TestEncodeError(t *testing.T) {
	data := EncodeError(0x1007, "checksum mismatch")
	m := &ErrorMsg{}
	m.Decode(data)
	if m.Code != 0x1007 {
		t.Fatalf("code mismatch: got %d", m.Code)
	}
	if m.Message != "checksum mismatch" {
		t.Fatalf("message mismatch: got %q", m.Message)
	}
}
