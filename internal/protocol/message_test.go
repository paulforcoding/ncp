package protocol

import (
	"bytes"
	"testing"
)

func TestOpenMsg_Roundtrip(t *testing.T) {
	m := &OpenMsg{
		Path:  "/data/dir/file.txt",
		Flags: ProtoO_WRONLY | ProtoO_CREAT | ProtoO_TRUNC,
		Mode:  0o644,
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
		t.Fatalf("mode mismatch: got %o, want %o", m2.Mode, m.Mode)
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
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m2.FD != m.FD {
		t.Fatalf("fd mismatch: got %d, want %d", m2.FD, m.FD)
	}
}

func TestCloseMsg_Roundtrip(t *testing.T) {
	checksum := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	m := &CloseMsg{FD: 3, Checksum: checksum}
	data := m.Encode()

	m2 := &CloseMsg{}
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
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
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
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
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
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
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
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
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
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

func TestChmodMsg_Roundtrip(t *testing.T) {
	m := &ChmodMsg{Path: "bin/sudo", Mode: 0o4755}
	data := m.Encode()

	m2 := &ChmodMsg{}
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m2.Path != m.Path {
		t.Fatalf("path mismatch: got %q, want %q", m2.Path, m.Path)
	}
	if m2.Mode != m.Mode {
		t.Fatalf("mode mismatch: got %o, want %o", m2.Mode, m.Mode)
	}
}

func TestChownMsg_Roundtrip(t *testing.T) {
	m := &ChownMsg{Path: "data/file.txt", UID: 1000, GID: 1000}
	data := m.Encode()

	m2 := &ChownMsg{}
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m2.Path != m.Path {
		t.Fatalf("path mismatch: got %q, want %q", m2.Path, m.Path)
	}
	if m2.UID != m.UID {
		t.Fatalf("uid mismatch: got %d, want %d", m2.UID, m.UID)
	}
	if m2.GID != m.GID {
		t.Fatalf("gid mismatch: got %d, want %d", m2.GID, m.GID)
	}
}

func TestTaskDoneMsg_Roundtrip(t *testing.T) {
	m := &TaskDoneMsg{}
	data := m.Encode()
	if len(data) != 0 {
		t.Fatalf("expected empty payload, got %d bytes", len(data))
	}
	m2 := &TaskDoneMsg{}
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
}

func TestAckMsg_Roundtrip(t *testing.T) {
	m := &AckMsg{ResultCode: 0, Data: []byte{0, 0, 0, 42}}
	data := m.Encode()

	m2 := &AckMsg{}
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
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
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
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
	if err := ack.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	_, fd := DecodeAckFD(ack.Data)
	if fd != 42 {
		t.Fatalf("fd mismatch: got %d, want 42", fd)
	}
}

func TestEncodeError(t *testing.T) {
	data := EncodeError(0x1007, "checksum mismatch")
	m := &ErrorMsg{}
	if err := m.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m.Code != 0x1007 {
		t.Fatalf("code mismatch: got %d", m.Code)
	}
	if m.Message != "checksum mismatch" {
		t.Fatalf("message mismatch: got %q", m.Message)
	}
}

// --- New messages for ncp:// source ---

func TestInitMsg_Roundtrip(t *testing.T) {
	m := &InitMsg{BasePath: "/data/backup", Mode: 1, TaskID: "task-001", ConfigJSON: `{"ProgramLogLevel":"info"}`}
	data := m.Encode()

	m2 := &InitMsg{}
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m2.BasePath != m.BasePath {
		t.Fatalf("basePath mismatch: got %q, want %q", m2.BasePath, m.BasePath)
	}
	if m2.Mode != m.Mode {
		t.Fatalf("mode mismatch: got %d, want %d", m2.Mode, m.Mode)
	}
	if m2.TaskID != m.TaskID {
		t.Fatalf("taskID mismatch: got %q, want %q", m2.TaskID, m.TaskID)
	}
	if m2.ConfigJSON != m.ConfigJSON {
		t.Fatalf("configJSON mismatch: got %q, want %q", m2.ConfigJSON, m.ConfigJSON)
	}
}

func TestInitMsg_EmptyFields(t *testing.T) {
	m := &InitMsg{BasePath: "", ConfigJSON: ""}
	data := m.Encode()

	m2 := &InitMsg{}
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m2.BasePath != "" {
		t.Fatalf("expected empty basePath, got %q", m2.BasePath)
	}
	if m2.ConfigJSON != "" {
		t.Fatalf("expected empty configJSON, got %q", m2.ConfigJSON)
	}
}

func TestInitMsg_MissingConfigJSON(t *testing.T) {
	// Protocol v3 data (no ConfigJSON field) should fail to decode
	data := []byte{0, 5, 'h', 'e', 'l', 'l', 'o', 1, 0, 4, 't', 'e', 's', 't'} // BasePath="hello", Mode=1, TaskID="test"
	m := &InitMsg{}
	if err := m.Decode(data); err == nil {
		t.Fatal("expected error for missing configJSON field")
	}
}

func TestListMsg_Roundtrip(t *testing.T) {
	m := &ListMsg{ContinuationToken: "1000"}
	data := m.Encode()

	m2 := &ListMsg{}
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m2.ContinuationToken != m.ContinuationToken {
		t.Fatalf("token mismatch: got %q, want %q", m2.ContinuationToken, m.ContinuationToken)
	}
}

func TestListMsg_EmptyToken(t *testing.T) {
	m := &ListMsg{ContinuationToken: ""}
	data := m.Encode()

	m2 := &ListMsg{}
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m2.ContinuationToken != "" {
		t.Fatalf("expected empty token, got %q", m2.ContinuationToken)
	}
}

func TestPreadMsg_Roundtrip(t *testing.T) {
	m := &PreadMsg{FD: 7, Offset: 4096, Length: 8192}
	data := m.Encode()

	m2 := &PreadMsg{}
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m2.FD != m.FD {
		t.Fatalf("fd mismatch: got %d, want %d", m2.FD, m.FD)
	}
	if m2.Offset != m.Offset {
		t.Fatalf("offset mismatch: got %d, want %d", m2.Offset, m.Offset)
	}
	if m2.Length != m.Length {
		t.Fatalf("length mismatch: got %d, want %d", m2.Length, m.Length)
	}
}

func TestStatMsg_Roundtrip(t *testing.T) {
	m := &StatMsg{RelPath: "subdir/file.txt"}
	data := m.Encode()

	m2 := &StatMsg{}
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m2.RelPath != m.RelPath {
		t.Fatalf("relpath mismatch: got %q, want %q", m2.RelPath, m.RelPath)
	}
}

func TestListEntry_Roundtrip(t *testing.T) {
	m := &ListEntry{
		RelPath:    "dir/file.txt",
		FileType:   1,
		FileSize:   1024,
		Mode:       0o644,
		Mtime:      1700000000000000000,
		LinkTarget: "",
		ETag:       "abc123",
		UID:        1000,
		GID:        1000,
	}
	data := m.Encode()

	m2 := &ListEntry{}
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m2.RelPath != m.RelPath {
		t.Fatalf("relpath mismatch: got %q, want %q", m2.RelPath, m.RelPath)
	}
	if m2.FileType != m.FileType {
		t.Fatalf("filetype mismatch: got %d, want %d", m2.FileType, m.FileType)
	}
	if m2.FileSize != m.FileSize {
		t.Fatalf("filesize mismatch: got %d, want %d", m2.FileSize, m.FileSize)
	}
	if m2.Mode != m.Mode {
		t.Fatalf("mode mismatch: got %o, want %o", m2.Mode, m.Mode)
	}
	if m2.Mtime != m.Mtime {
		t.Fatalf("mtime mismatch: got %d, want %d", m2.Mtime, m.Mtime)
	}
	if m2.ETag != m.ETag {
		t.Fatalf("etag mismatch: got %q, want %q", m2.ETag, m.ETag)
	}
	if m2.UID != m.UID {
		t.Fatalf("uid mismatch: got %d, want %d", m2.UID, m.UID)
	}
	if m2.GID != m.GID {
		t.Fatalf("gid mismatch: got %d, want %d", m2.GID, m.GID)
	}
}

func TestListEntry_SetuidMode(t *testing.T) {
	m := &ListEntry{
		RelPath:  "bin/sudo",
		FileType: 1,
		FileSize: 8192,
		Mode:     0o4755, // setuid + rwxr-xr-x
		Mtime:    1700000000000000000,
		UID:      0,
		GID:      0,
	}
	data := m.Encode()

	m2 := &ListEntry{}
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m2.Mode != 0o4755 {
		t.Fatalf("mode mismatch: got %o, want 4755", m2.Mode)
	}
}

func TestListEntry_Symlink(t *testing.T) {
	m := &ListEntry{
		RelPath:    "link",
		FileType:   3,
		FileSize:   0,
		Mode:       0777,
		Mtime:      1700000000000000000,
		LinkTarget: "target.txt",
		ETag:       "",
	}
	data := m.Encode()

	m2 := &ListEntry{}
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m2.LinkTarget != m.LinkTarget {
		t.Fatalf("linktarget mismatch: got %q, want %q", m2.LinkTarget, m.LinkTarget)
	}
}

func TestDataMsg_WithEntries(t *testing.T) {
	m := &DataMsg{
		ResultCode: 0,
		Entries: []ListEntry{
			{RelPath: "file1.txt", FileType: 1, FileSize: 100, Mode: 0644, Mtime: 1700000000},
			{RelPath: "dir1", FileType: 2, FileSize: 0, Mode: 0755, Mtime: 1700000001},
		},
		ContinuationToken: "1000",
		Data:              nil,
	}
	data := m.Encode()

	m2 := &DataMsg{}
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m2.ResultCode != m.ResultCode {
		t.Fatalf("resultcode mismatch: got %d, want %d", m2.ResultCode, m.ResultCode)
	}
	if len(m2.Entries) != 2 {
		t.Fatalf("entry count: got %d, want 2", len(m2.Entries))
	}
	if m2.Entries[0].RelPath != "file1.txt" {
		t.Fatalf("entry[0] relpath: got %q", m2.Entries[0].RelPath)
	}
	if m2.Entries[1].RelPath != "dir1" {
		t.Fatalf("entry[1] relpath: got %q", m2.Entries[1].RelPath)
	}
	if m2.ContinuationToken != "1000" {
		t.Fatalf("token mismatch: got %q", m2.ContinuationToken)
	}
}

func TestDataMsg_WithData(t *testing.T) {
	payload := []byte("hello world from pread")
	m := &DataMsg{
		ResultCode:        0,
		Entries:           nil,
		ContinuationToken: "",
		Data:              payload,
	}
	data := m.Encode()

	m2 := &DataMsg{}
	if err := m2.Decode(data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(m2.Data, payload) {
		t.Fatalf("data mismatch: got %q, want %q", string(m2.Data), string(payload))
	}
	if len(m2.Entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(m2.Entries))
	}
}
