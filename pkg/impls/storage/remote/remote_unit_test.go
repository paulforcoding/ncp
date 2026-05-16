package remote

import (
	"os"
	"strings"
	"testing"

	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/pkg/model"
)

// --- Pure conversion function tests ---

func TestOsFlagsToProto(t *testing.T) {
	tests := []struct {
		name  string
		flags int
		want  uint32
	}{
		{"O_WRONLY", os.O_WRONLY, protocol.ProtoO_WRONLY},
		{"O_RDWR|O_CREATE|O_TRUNC", os.O_RDWR | os.O_CREATE | os.O_TRUNC,
			protocol.ProtoO_RDWR | protocol.ProtoO_CREAT | protocol.ProtoO_TRUNC},
		{"O_WRONLY|O_CREATE|O_APPEND", os.O_WRONLY | os.O_CREATE | os.O_APPEND,
			protocol.ProtoO_WRONLY | protocol.ProtoO_CREAT | protocol.ProtoO_APPEND},
		{"O_RDONLY", os.O_RDONLY, 0},
		{"zero", 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := osFlagsToProto(tt.flags)
			if got != tt.want {
				t.Errorf("osFlagsToProto(%#x) = %#x, want %#x", tt.flags, got, tt.want)
			}
		})
	}
}

func TestProtoModeToOS(t *testing.T) {
	tests := []struct {
		name string
		pm   uint32
		want os.FileMode
	}{
		{"plain 0755", 0o755, os.FileMode(0o755)},
		{"with setuid", 0o4755, os.FileMode(0o4755)},
		{"with setgid", 0o2755, os.FileMode(0o2755)},
		{"with sticky", 0o1755, os.FileMode(0o1755)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := protoModeToOS(tt.pm)
			if got != tt.want {
				t.Errorf("protoModeToOS(%#o) = %#o, want %#o", tt.pm, got, tt.want)
			}
		})
	}
}

func TestOsModeToProto(t *testing.T) {
	tests := []struct {
		name string
		mode os.FileMode
		want uint32
	}{
		{"plain 0755", 0o755, 0o755},
		{"setuid", os.ModeSetuid | 0o755, protocol.ProtoModeSetuid | 0o755},
		{"setgid", os.ModeSetgid | 0o755, protocol.ProtoModeSetgid | 0o755},
		{"sticky", os.ModeSticky | 0o755, protocol.ProtoModeSticky | 0o755},
		{"all special", os.ModeSetuid | os.ModeSetgid | os.ModeSticky | 0o777,
			protocol.ProtoModeSetuid | protocol.ProtoModeSetgid | protocol.ProtoModeSticky | 0o777},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := osModeToProto(tt.mode)
			if got != tt.want {
				t.Errorf("osModeToProto(%#o) = %#o, want %#o", tt.mode, got, tt.want)
			}
		})
	}
}

func TestListEntryToDiscoverItem(t *testing.T) {
	entry := &protocol.ListEntry{
		RelPath:    "sub/file.txt",
		FileType:   uint8(model.FileRegular),
		FileSize:   1024,
		Mode:       0o644,
		Mtime:      1234567890000000000,
		UID:        1000,
		GID:        1000,
		LinkTarget: "",
		ETag:       "",
	}

	item := listEntryToDiscoverItem(entry)
	if item.RelPath != "sub/file.txt" {
		t.Errorf("RelPath = %q", item.RelPath)
	}
	if item.FileType != model.FileRegular {
		t.Errorf("FileType = %v", item.FileType)
	}
	if item.Size != 1024 {
		t.Errorf("Size = %d", item.Size)
	}
	if item.Attr.Uid != 1000 || item.Attr.Gid != 1000 {
		t.Errorf("Uid=%d Gid=%d", item.Attr.Uid, item.Attr.Gid)
	}
}

func TestListEntryToDiscoverItem_Symlink(t *testing.T) {
	entry := &protocol.ListEntry{
		RelPath:    "link",
		FileType:   uint8(model.FileSymlink),
		FileSize:   0,
		Mode:       0o777,
		LinkTarget: "/target/path",
	}

	item := listEntryToDiscoverItem(entry)
	if item.FileType != model.FileSymlink {
		t.Errorf("FileType = %v, want FileSymlink", item.FileType)
	}
	if item.Attr.SymlinkTarget != "/target/path" {
		t.Errorf("SymlinkTarget = %q", item.Attr.SymlinkTarget)
	}
}

func TestListEntryToDiscoverItem_Directory(t *testing.T) {
	entry := &protocol.ListEntry{
		RelPath:  "subdir",
		FileType: uint8(model.FileDir),
		FileSize: 0,
		Mode:     0o755,
	}

	item := listEntryToDiscoverItem(entry)
	if item.FileType != model.FileDir {
		t.Errorf("FileType = %v, want FileDir", item.FileType)
	}
}

func TestParseETag(t *testing.T) {
	tests := []struct {
		name      string
		etag      string
		wantAlgo  string
		wantEmpty bool
	}{
		{"plain md5", "d41d8cd98f00b204e9800998ecf8427e", "etag-md5", false},
		{"quoted md5", `"d41d8cd98f00b204e9800998ecf8427e"`, "etag-md5", false},
		{"multipart", "abcdef-3", "etag-multipart", false},
		{"empty", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cksum, algo := parseETag(tt.etag)
			if tt.wantEmpty {
				if cksum != nil || algo != "" {
					t.Errorf("expected nil/empty, got cksum=%v algo=%q", cksum, algo)
				}
				return
			}
			if algo != tt.wantAlgo {
				t.Errorf("algo = %q, want %q", algo, tt.wantAlgo)
			}
			if cksum == nil {
				t.Error("expected non-nil checksum")
			}
		})
	}
}

func TestSourceURI(t *testing.T) {
	s, _ := NewSource("host:9900", "/data/backup")
	got := s.URI()
	want := "ncp://host:9900/data/backup"
	if got != want {
		t.Errorf("URI() = %q, want %q", got, want)
	}
}

func TestNewSource_WithMode(t *testing.T) {
	s, err := NewSource("host:9900", "/data", WithMode(protocol.ModeSourceNoWalker))
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	if s.mode != protocol.ModeSourceNoWalker {
		t.Errorf("mode = %d, want %d", s.mode, protocol.ModeSourceNoWalker)
	}
}

func TestNewSource_DefaultMode(t *testing.T) {
	s, err := NewSource("host:9900", "/data")
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	if s.mode != protocol.ModeSource {
		t.Errorf("default mode = %d, want %d", s.mode, protocol.ModeSource)
	}
}

func TestDestinationFullPath(t *testing.T) {
	d, _ := NewDestination("host:9900", "/data")
	got := d.fullPath("sub/file.txt")
	// fullPath returns relPath as-is (basePath already sent via MsgInit)
	if got != "sub/file.txt" {
		t.Errorf("fullPath = %q, want %q", got, "sub/file.txt")
	}
}

func TestNewDestination_Basic(t *testing.T) {
	d, err := NewDestination("host:9900", "/data/backup")
	if err != nil {
		t.Fatalf("NewDestination: %v", err)
	}
	if d.addr != "host:9900" || d.basePath != "/data/backup" {
		t.Errorf("addr=%q basePath=%q", d.addr, d.basePath)
	}
}

// --- Writer state machine tests ---

func TestWriter_WriteAfterCommit(t *testing.T) {
	w := &Writer{fd: 1, committed: true}
	_, err := w.Write(nil, []byte("x"))
	if err == nil {
		t.Fatal("expected error writing to committed writer")
	}
	if !strings.Contains(err.Error(), "closed writer") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestWriter_WriteAfterAbort(t *testing.T) {
	w := &Writer{fd: 1, aborted: true}
	_, err := w.Write(nil, []byte("x"))
	if err == nil {
		t.Fatal("expected error writing to aborted writer")
	}
}

func TestWriter_DoubleCommitIsNoop(t *testing.T) {
	w := &Writer{fd: 1, committed: true}
	if err := w.Commit(nil, nil); err != nil {
		t.Errorf("second Commit should return nil, got %v", err)
	}
}

func TestWriter_DoubleAbortIsNoop(t *testing.T) {
	w := &Writer{fd: 1, aborted: true}
	if err := w.Abort(nil); err != nil {
		t.Errorf("second Abort should return nil, got %v", err)
	}
}

func TestWriter_CommitAfterAbort(t *testing.T) {
	w := &Writer{fd: 1, aborted: true, committed: false}
	if err := w.Commit(nil, nil); err != nil {
		t.Errorf("Commit after Abort should return nil, got %v", err)
	}
}

func TestWriter_BytesWritten(t *testing.T) {
	w := &Writer{fd: 1, bytesWritten: 1024}
	if got := w.BytesWritten(); got != 1024 {
		t.Errorf("BytesWritten = %d, want 1024", got)
	}
}

// Compile-time checks
var _ = NewSource
var _ = NewDestination
