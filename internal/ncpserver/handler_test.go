package ncpserver

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/pkg/model"
)

// --- Pure conversion function tests ---

func TestProtoFlagsToOS(t *testing.T) {
	tests := []struct {
		name string
		pf   uint32
		want int
	}{
		{"WRONLY", protocol.ProtoO_WRONLY, os.O_WRONLY},
		{"RDWR|CREAT|TRUNC", protocol.ProtoO_RDWR | protocol.ProtoO_CREAT | protocol.ProtoO_TRUNC, os.O_RDWR | os.O_CREATE | os.O_TRUNC},
		{"WRONLY|CREAT|APPEND", protocol.ProtoO_WRONLY | protocol.ProtoO_CREAT | protocol.ProtoO_APPEND, os.O_WRONLY | os.O_CREATE | os.O_APPEND},
		{"zero (RDONLY)", 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := protoFlagsToOS(tt.pf)
			if got != tt.want {
				t.Errorf("protoFlagsToOS(%#x) = %#x, want %#x", tt.pf, got, tt.want)
			}
		})
	}
}

func TestProtoToOsMode(t *testing.T) {
	tests := []struct {
		name string
		pm   uint32
		want os.FileMode
	}{
		{"plain", 0o755, os.FileMode(0o755)},
		{"setuid", 0o4755, os.ModeSetuid | os.FileMode(0o755)},
		{"setgid", 0o2755, os.ModeSetgid | os.FileMode(0o755)},
		{"sticky", 0o1755, os.ModeSticky | os.FileMode(0o755)},
		{"all bits", 0o7777, os.ModeSetuid | os.ModeSetgid | os.ModeSticky | os.FileMode(0o777)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := protoToOsMode(tt.pm)
			if got != tt.want {
				t.Errorf("protoToOsMode(%#o) = %#o, want %#o", tt.pm, got, tt.want)
			}
		})
	}
}

func TestOsModeToProto_RoundTrip(t *testing.T) {
	tests := []os.FileMode{
		0o755,
		os.ModeSetuid | 0o755,
		os.ModeSetgid | 0o755,
		os.ModeSticky | 0o755,
		os.ModeSetuid | os.ModeSetgid | os.ModeSticky | 0o777,
		0,
	}
	for _, mode := range tests {
		proto := osModeToProto(mode)
		back := protoToOsMode(proto)
		if mode != back {
			t.Errorf("round-trip failed: %#o → %#o → %#o", mode, proto, back)
		}
	}
}

func TestEqualChecksum(t *testing.T) {
	tests := []struct {
		name string
		a, b []byte
		want bool
	}{
		{"equal", []byte{1, 2, 3}, []byte{1, 2, 3}, true},
		{"different", []byte{1, 2, 3}, []byte{1, 2, 4}, false},
		{"different length", []byte{1, 2}, []byte{1, 2, 3}, false},
		{"both nil", nil, nil, true},
		{"one nil", nil, []byte{1}, false},
		{"empty", []byte{}, []byte{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := equalChecksum(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("equalChecksum(%x, %x) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// --- File operation tests ---

func TestFullPath(t *testing.T) {
	h := &ConnHandler{basePath: "/tmp/base"}
	got := h.fullPath("sub/file.txt")
	want := filepath.Join("/tmp/base", "sub/file.txt")
	if got != want {
		t.Errorf("fullPath = %q, want %q", got, want)
	}
}

func TestInfoToListEntry_RegularFile(t *testing.T) {
	dir := t.TempDir()
	fpath := filepath.Join(dir, "test.txt")
	if err := os.WriteFile(fpath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	// Set mtime to a known value
	knownTime := time.Unix(1234567890, 0)
	if err := os.Chtimes(fpath, knownTime, knownTime); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	info, err := os.Stat(fpath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	entry := infoToListEntry("test.txt", info, fpath)
	if entry.RelPath != "test.txt" {
		t.Errorf("RelPath = %q, want test.txt", entry.RelPath)
	}
	if entry.FileType != uint8(model.FileRegular) {
		t.Errorf("FileType = %d, want %d", entry.FileType, model.FileRegular)
	}
	if entry.FileSize != 5 {
		t.Errorf("FileSize = %d, want 5", entry.FileSize)
	}
}

func TestInfoToListEntry_Directory(t *testing.T) {
	dir := t.TempDir()
	subDir := filepath.Join(dir, "subdir")
	if err := os.Mkdir(subDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	info, err := os.Stat(subDir)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	entry := infoToListEntry("subdir", info, subDir)
	if entry.RelPath != "subdir" {
		t.Errorf("RelPath = %q, want subdir", entry.RelPath)
	}
	if entry.FileType != uint8(model.FileDir) {
		t.Errorf("FileType = %d, want %d", entry.FileType, model.FileDir)
	}
}

func TestInfoToListEntry_Symlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.txt")
	link := filepath.Join(dir, "link.txt")
	if err := os.WriteFile(target, []byte("data"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Symlink("target.txt", link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	info, err := os.Lstat(link)
	if err != nil {
		t.Fatalf("lstat: %v", err)
	}

	entry := infoToListEntry("link.txt", info, link)
	if entry.FileType != uint8(model.FileSymlink) {
		t.Errorf("FileType = %d, want %d", entry.FileType, model.FileSymlink)
	}
	if entry.LinkTarget != "target.txt" {
		t.Errorf("LinkTarget = %q, want target.txt", entry.LinkTarget)
	}
}

// --- CleanupTempDir tests ---

func TestCleanupTempDir_NotExist(t *testing.T) {
	err := CleanupTempDir(filepath.Join(t.TempDir(), "nonexistent"))
	if err != nil {
		t.Errorf("expected nil for non-existent dir, got %v", err)
	}
}

func TestCleanupTempDir_Empty(t *testing.T) {
	dir := t.TempDir()
	err := CleanupTempDir(dir)
	if err != nil {
		t.Errorf("expected nil for empty dir, got %v", err)
	}
}

func TestCleanupTempDir_RemovesWalkerDirs(t *testing.T) {
	dir := t.TempDir()
	walkerDir := filepath.Join(dir, "walker-task-123")
	if err := os.Mkdir(walkerDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Place a file inside to ensure recursive removal
	if err := os.WriteFile(filepath.Join(walkerDir, "data"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	err := CleanupTempDir(dir)
	if err != nil {
		t.Errorf("CleanupTempDir: %v", err)
	}

	if _, err := os.Stat(walkerDir); !os.IsNotExist(err) {
		t.Errorf("walker dir should be removed, stat: %v", err)
	}
}

func TestCleanupTempDir_PreservesNonWalkerDirs(t *testing.T) {
	dir := t.TempDir()
	keepDir := filepath.Join(dir, "keep-me")
	if err := os.Mkdir(keepDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	err := CleanupTempDir(dir)
	if err != nil {
		t.Errorf("CleanupTempDir: %v", err)
	}

	if _, err := os.Stat(keepDir); err != nil {
		t.Errorf("non-walker dir should be preserved: %v", err)
	}
}

// --- Handler tests with net.Pipe ---

func TestHandler_OpenWrite(t *testing.T) {
	dir := t.TempDir()
	h := &ConnHandler{basePath: dir, fdWriteMap: make(map[uint32]*openWriteFile), fdReadMap: make(map[uint32]*os.File)}

	relPath := "sub/file.txt"
	msg := &protocol.OpenMsg{
		Path:  relPath,
		Flags: protocol.ProtoO_WRONLY | protocol.ProtoO_CREAT | protocol.ProtoO_TRUNC,
		Mode:  0o644,
	}

	frame := &protocol.Frame{Payload: msg.Encode()}
	respType, respPayload := h.handleOpen(frame)

	if respType != protocol.MsgAck {
		respErr := &protocol.ErrorMsg{}
		respErr.Decode(respPayload)
		t.Fatalf("expected Ack, got type=0x%02x payload=%x (err=%v)", respType, respPayload, respErr)
	}

	fullPath := filepath.Join(dir, relPath)
	if _, err := os.Stat(fullPath); err != nil {
		t.Errorf("file should exist: %v", err)
	}
}

func TestHandler_OpenWriteThenPwrite(t *testing.T) {
	dir := t.TempDir()
	h := &ConnHandler{basePath: dir, fdWriteMap: make(map[uint32]*openWriteFile), fdReadMap: make(map[uint32]*os.File)}

	// Open
	openMsg := &protocol.OpenMsg{
		Path:  "data.txt",
		Flags: protocol.ProtoO_WRONLY | protocol.ProtoO_CREAT | protocol.ProtoO_TRUNC,
		Mode:  0o644,
	}
	respType, respPayload := h.handleOpen(&protocol.Frame{Payload: openMsg.Encode()})
	if respType != protocol.MsgAck {
		t.Fatalf("open: expected Ack, got 0x%02x", respType)
	}
	_, fd := protocol.DecodeAckFD(respPayload)

	// Pwrite
	pwriteMsg := &protocol.PwriteMsg{FD: fd, Offset: 0, Data: []byte("hello world")}
	respType, respPayload = h.handlePwrite(&protocol.Frame{Payload: pwriteMsg.Encode()})
	if respType != protocol.MsgAck {
		t.Fatalf("pwrite: expected Ack, got 0x%02x", respType)
	}

	// Verify content on disk
	fullPath := filepath.Join(dir, "data.txt")
	data, _ := os.ReadFile(fullPath)
	if string(data) != "hello world" {
		t.Errorf("file content = %q, want %q", string(data), "hello world")
	}
}

func TestHandler_OpenWriteThenPwriteThenClose(t *testing.T) {
	dir := t.TempDir()
	h := &ConnHandler{basePath: dir, fdWriteMap: make(map[uint32]*openWriteFile), fdReadMap: make(map[uint32]*os.File)}

	// Open
	openMsg := &protocol.OpenMsg{
		Path:  "data.txt",
		Flags: protocol.ProtoO_WRONLY | protocol.ProtoO_CREAT | protocol.ProtoO_TRUNC,
		Mode:  0o644,
	}
	_, respPayload := h.handleOpen(&protocol.Frame{Payload: openMsg.Encode()})
	_, fd := protocol.DecodeAckFD(respPayload)

	// Pwrite
	pwriteMsg := &protocol.PwriteMsg{FD: fd, Offset: 0, Data: []byte("test data")}
	h.handlePwrite(&protocol.Frame{Payload: pwriteMsg.Encode()})

	// Get server-side MD5 so we can send matching checksum
	of := h.fdWriteMap[fd]
	serverMD5 := of.md5.Sum(nil)

	// Close with correct checksum
	closeMsg := &protocol.CloseMsg{FD: fd, Checksum: serverMD5}
	respType, _ := h.handleClose(&protocol.Frame{Payload: closeMsg.Encode()})
	if respType != protocol.MsgAck {
		t.Errorf("close with correct checksum: expected Ack, got 0x%02x", respType)
	}

	// FD should be cleaned up
	if _, ok := h.fdWriteMap[fd]; ok {
		t.Error("fd should be removed after close")
	}
}

func TestHandler_Close_BadChecksum(t *testing.T) {
	dir := t.TempDir()
	h := &ConnHandler{basePath: dir, fdWriteMap: make(map[uint32]*openWriteFile), fdReadMap: make(map[uint32]*os.File)}

	// Open
	openMsg := &protocol.OpenMsg{
		Path:  "data.txt",
		Flags: protocol.ProtoO_WRONLY | protocol.ProtoO_CREAT | protocol.ProtoO_TRUNC,
		Mode:  0o644,
	}
	_, respPayload := h.handleOpen(&protocol.Frame{Payload: openMsg.Encode()})
	_, fd := protocol.DecodeAckFD(respPayload)

	// Pwrite
	pwriteMsg := &protocol.PwriteMsg{FD: fd, Offset: 0, Data: []byte("test data")}
	h.handlePwrite(&protocol.Frame{Payload: pwriteMsg.Encode()})

	// Close with wrong checksum
	closeMsg := &protocol.CloseMsg{FD: fd, Checksum: []byte{0xde, 0xad, 0xbe, 0xef}}
	respType, _ := h.handleClose(&protocol.Frame{Payload: closeMsg.Encode()})
	if respType != protocol.MsgError {
		t.Errorf("close with wrong checksum: expected Error, got 0x%02x", respType)
	}
}

func TestHandler_Mkdir(t *testing.T) {
	dir := t.TempDir()
	h := &ConnHandler{basePath: dir}

	msg := &protocol.MkdirMsg{
		Path: "newdir",
		Mode: 0o755,
	}
	respType, _ := h.handleMkdir(&protocol.Frame{Payload: msg.Encode()})
	if respType != protocol.MsgAck {
		t.Errorf("expected Ack, got 0x%02x", respType)
	}

	fullPath := filepath.Join(dir, "newdir")
	info, err := os.Stat(fullPath)
	if err != nil {
		t.Fatalf("dir should exist: %v", err)
	}
	if !info.IsDir() {
		t.Error("expected directory")
	}
}

func TestHandler_AbortFile(t *testing.T) {
	dir := t.TempDir()
	h := &ConnHandler{basePath: dir, fdWriteMap: make(map[uint32]*openWriteFile), fdReadMap: make(map[uint32]*os.File)}

	// Open
	openMsg := &protocol.OpenMsg{
		Path:  "data.txt",
		Flags: protocol.ProtoO_WRONLY | protocol.ProtoO_CREAT | protocol.ProtoO_TRUNC,
		Mode:  0o644,
	}
	_, respPayload := h.handleOpen(&protocol.Frame{Payload: openMsg.Encode()})
	_, fd := protocol.DecodeAckFD(respPayload)

	// Abort
	abortMsg := &protocol.AbortFileMsg{FD: fd}
	respType, _ := h.handleAbortFile(&protocol.Frame{Payload: abortMsg.Encode()})
	if respType != protocol.MsgAck {
		t.Errorf("expected Ack, got 0x%02x", respType)
	}

	// File should be removed
	fullPath := filepath.Join(dir, "data.txt")
	if _, err := os.Stat(fullPath); !os.IsNotExist(err) {
		t.Errorf("file should be removed after abort, stat: %v", err)
	}

	// FD should be cleaned up
	if _, ok := h.fdWriteMap[fd]; ok {
		t.Error("fd should be removed after abort")
	}
}

func TestHandler_Pwrite_BadFD(t *testing.T) {
	h := &ConnHandler{basePath: "/tmp", fdWriteMap: make(map[uint32]*openWriteFile), fdReadMap: make(map[uint32]*os.File)}

	msg := &protocol.PwriteMsg{FD: 999, Offset: 0, Data: []byte("x")}
	respType, _ := h.handlePwrite(&protocol.Frame{Payload: msg.Encode()})
	if respType != protocol.MsgError {
		t.Errorf("expected Error for bad fd, got 0x%02x", respType)
	}
}

// Compile-time check: ConnHandler is the handler type
var _ = fmt.Sprintf
