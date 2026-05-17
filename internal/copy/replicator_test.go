package copy

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zp001/ncp/pkg/impls/storage/local"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

func TestReplicatorCopyDir(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	if err := os.MkdirAll(filepath.Join(srcDir, "subdir"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewDestination(dstDir)

	r := NewReplicator(0, src, dst, nil, 0, model.CksumMD5, &ThroughputMeter{}, false, 0)
	item := storage.DiscoverItem{RelPath: "subdir", FileType: model.FileDir, Attr: storage.FileAttr{Mode: 0o755}}
	result := r.copyOne(context.Background(), item)

	if result.CopyStatus != model.CopyDone {
		t.Errorf("expected CopyDone, got %v", result.CopyStatus)
	}
	if _, err := os.Stat(filepath.Join(dstDir, "subdir")); err != nil {
		t.Errorf("subdir not created: %v", err)
	}
}

func TestReplicatorCopySymlink(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(srcDir, "target.txt"), []byte("data"), 0o644); err != nil {
		t.Fatalf("write target: %v", err)
	}
	if err := os.Symlink("target.txt", filepath.Join(srcDir, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewDestination(dstDir)

	r := NewReplicator(0, src, dst, nil, 0, model.CksumMD5, &ThroughputMeter{}, false, 0)
	item := storage.DiscoverItem{RelPath: "link", FileType: model.FileSymlink, Attr: storage.FileAttr{SymlinkTarget: "target.txt"}}
	result := r.copyOne(context.Background(), item)

	if result.CopyStatus != model.CopyDone {
		t.Errorf("expected CopyDone, got %v", result.CopyStatus)
	}

	target, err := os.Readlink(filepath.Join(dstDir, "link"))
	if err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if target != "target.txt" {
		t.Errorf("expected target 'target.txt', got %q", target)
	}
}

func TestReplicatorCopyFile(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(srcDir, "file.txt"), []byte("hello world"), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewDestination(dstDir)

	r := NewReplicator(0, src, dst, nil, 0, model.CksumMD5, &ThroughputMeter{}, false, 0)
	item := storage.DiscoverItem{RelPath: "file.txt", FileType: model.FileRegular, Size: 11, Attr: storage.FileAttr{Mode: 0o644}}
	result := r.copyOne(context.Background(), item)

	if result.CopyStatus != model.CopyDone {
		t.Errorf("expected CopyDone, got %v: %v", result.CopyStatus, result.Err)
	}
	if result.Checksum == "" {
		t.Error("expected non-empty checksum")
	}

	data, err := os.ReadFile(filepath.Join(dstDir, "file.txt"))
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if string(data) != "hello world" {
		t.Errorf("expected 'hello world', got %q", string(data))
	}
}

func TestReplicatorCopyFileErrorNotExist(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewDestination(dstDir)

	r := NewReplicator(0, src, dst, nil, 0, model.CksumMD5, &ThroughputMeter{}, false, 0)
	item := storage.DiscoverItem{RelPath: "nonexistent.txt", FileType: model.FileRegular, Size: 100, Attr: storage.FileAttr{Mode: 0o644}}
	result := r.copyOne(context.Background(), item)

	if result.CopyStatus != model.CopyError {
		t.Errorf("expected CopyError for missing file, got %v", result.CopyStatus)
	}
	if result.Err == nil {
		t.Error("expected error for missing file, got nil")
	}
}

func TestReplicatorEmptySymlinkTarget(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewDestination(dstDir)

	r := NewReplicator(0, src, dst, nil, 0, model.CksumMD5, &ThroughputMeter{}, false, 0)
	item := storage.DiscoverItem{RelPath: "link", FileType: model.FileSymlink}
	result := r.copyOne(context.Background(), item)

	if result.CopyStatus != model.CopyError {
		t.Errorf("expected CopyError for empty symlink target, got %v", result.CopyStatus)
	}
	if result.Err == nil {
		t.Error("expected error for empty symlink target")
	}
}

func TestReplicatorUnknownFileType(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewDestination(dstDir)

	r := NewReplicator(0, src, dst, nil, 0, model.CksumMD5, &ThroughputMeter{}, false, 0)
	item := storage.DiscoverItem{RelPath: "unknown", FileType: model.FileType(255)}
	result := r.copyOne(context.Background(), item)

	if result.CopyStatus != model.CopyError {
		t.Errorf("expected CopyError for unknown type, got %v", result.CopyStatus)
	}
	if result.Err == nil {
		t.Error("expected error for unknown file type")
	}
}

func TestReplicatorCopyFileWithChecksum(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	content := []byte("test content for checksum")
	if err := os.WriteFile(filepath.Join(srcDir, "file.txt"), content, 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewDestination(dstDir)

	r := NewReplicator(0, src, dst, nil, 0, model.CksumMD5, &ThroughputMeter{}, false, 0)
	item := storage.DiscoverItem{RelPath: "file.txt", FileType: model.FileRegular, Size: int64(len(content)), Attr: storage.FileAttr{Mode: 0o644}}
	result := r.copyOne(context.Background(), item)

	if result.CopyStatus != model.CopyDone {
		t.Fatalf("expected CopyDone, got %v: %v", result.CopyStatus, result.Err)
	}
	if result.Checksum == "" {
		t.Error("expected non-empty checksum")
	}
	if result.Algorithm != "md5" {
		t.Errorf("expected algorithm 'md5', got %q", result.Algorithm)
	}
}

func TestReplicatorSkipByMtime(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	content := []byte("same content")
	if err := os.WriteFile(filepath.Join(srcDir, "file.txt"), content, 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dstDir, "file.txt"), content, 0o644); err != nil {
		t.Fatalf("write dst: %v", err)
	}
	// Ensure identical mtime for skip-by-mtime to trigger
	now := time.Now().Truncate(time.Second)
	if err := os.Chtimes(filepath.Join(srcDir, "file.txt"), now, now); err != nil {
		t.Fatalf("chtimes src: %v", err)
	}
	if err := os.Chtimes(filepath.Join(dstDir, "file.txt"), now, now); err != nil {
		t.Fatalf("chtimes dst: %v", err)
	}

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewDestination(dstDir)

	r := NewReplicator(0, src, dst, nil, 0, model.CksumMD5, &ThroughputMeter{}, true, 0)
	item := storage.DiscoverItem{RelPath: "file.txt", FileType: model.FileRegular, Size: int64(len(content)), Attr: storage.FileAttr{Mode: 0o644, Mtime: now}}
	result := r.copyOne(context.Background(), item)

	if result.CopyStatus != model.CopyDone {
		t.Errorf("expected CopyDone, got %v", result.CopyStatus)
	}
	if !result.Skipped {
		t.Error("expected Skipped=true for skip-by-mtime")
	}
}
