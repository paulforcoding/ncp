package cksum

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zp001/ncp/pkg/impls/storage/local"
	"github.com/zp001/ncp/pkg/model"
)

func TestCksumWorkerUnknownFileType(t *testing.T) {
	w := NewCksumWorker(0, nil, nil, nil, 0, model.CksumMD5, false)
	result := w.cksumOne(model.DiscoverItem{FileType: model.FileType(255), RelPath: "unknown"})
	if result.CksumStatus != model.CksumError {
		t.Errorf("expected CksumError for unknown type, got %v", result.CksumStatus)
	}
}

func TestCksumWorkerCksumDirPass(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	os.MkdirAll(filepath.Join(srcDir, "subdir"), 0o755)
	os.MkdirAll(filepath.Join(dstDir, "subdir"), 0o755)

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewSource(dstDir)

	w := NewCksumWorker(0, src, dst, nil, 0, model.CksumMD5, false)
	result := w.cksumOne(model.DiscoverItem{RelPath: "subdir", FileType: model.FileDir})

	if result.CksumStatus != model.CksumPass {
		t.Errorf("expected CksumPass for dir, got %v", result.CksumStatus)
	}
}

func TestCksumWorkerCksumDirMismatch(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	os.MkdirAll(filepath.Join(srcDir, "subdir"), 0o755)
	os.WriteFile(filepath.Join(dstDir, "subdir"), []byte("not a dir"), 0o644)

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewSource(dstDir)

	w := NewCksumWorker(0, src, dst, nil, 0, model.CksumMD5, false)
	result := w.cksumOne(model.DiscoverItem{RelPath: "subdir", FileType: model.FileDir})

	if result.CksumStatus != model.CksumMismatch {
		t.Errorf("expected CksumMismatch when dst is not dir, got %v", result.CksumStatus)
	}
}

func TestCksumWorkerCksumSymlinkPass(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	os.Symlink("target.txt", filepath.Join(srcDir, "link"))
	os.Symlink("target.txt", filepath.Join(dstDir, "link"))

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewSource(dstDir)

	w := NewCksumWorker(0, src, dst, nil, 0, model.CksumMD5, false)
	result := w.cksumOne(model.DiscoverItem{RelPath: "link", FileType: model.FileSymlink, LinkTarget: "target.txt"})

	if result.CksumStatus != model.CksumPass {
		t.Errorf("expected CksumPass for symlink, got %v", result.CksumStatus)
	}
}

func TestCksumWorkerCksumSymlinkMismatch(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	os.Symlink("target-a.txt", filepath.Join(srcDir, "link"))
	os.Symlink("target-b.txt", filepath.Join(dstDir, "link"))

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewSource(dstDir)

	w := NewCksumWorker(0, src, dst, nil, 0, model.CksumMD5, false)
	result := w.cksumOne(model.DiscoverItem{RelPath: "link", FileType: model.FileSymlink, LinkTarget: "target-a.txt"})

	if result.CksumStatus != model.CksumMismatch {
		t.Errorf("expected CksumMismatch for different symlink target, got %v", result.CksumStatus)
	}
}

func TestCksumWorkerCksumFilePass(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	os.WriteFile(filepath.Join(srcDir, "file.txt"), []byte("same content"), 0o644)
	os.WriteFile(filepath.Join(dstDir, "file.txt"), []byte("same content"), 0o644)

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewSource(dstDir)

	w := NewCksumWorker(0, src, dst, nil, 0, model.CksumMD5, false)
	result := w.cksumOne(model.DiscoverItem{RelPath: "file.txt", FileType: model.FileRegular, FileSize: 12})

	if result.CksumStatus != model.CksumPass {
		t.Errorf("expected CksumPass for identical file, got %v: %v", result.CksumStatus, result.Err)
	}
	if result.SrcHash == "" {
		t.Error("expected non-empty SrcHash")
	}
	if result.DstHash == "" {
		t.Error("expected non-empty DstHash")
	}
}

func TestCksumWorkerCksumFileMismatch(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	os.WriteFile(filepath.Join(srcDir, "file.txt"), []byte("content-a"), 0o644)
	os.WriteFile(filepath.Join(dstDir, "file.txt"), []byte("content-b"), 0o644)

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewSource(dstDir)

	w := NewCksumWorker(0, src, dst, nil, 0, model.CksumMD5, false)
	result := w.cksumOne(model.DiscoverItem{RelPath: "file.txt", FileType: model.FileRegular, FileSize: 9})

	if result.CksumStatus != model.CksumMismatch {
		t.Errorf("expected CksumMismatch for different content, got %v", result.CksumStatus)
	}
	if result.SrcHash == result.DstHash {
		t.Error("expected different hashes for different content")
	}
}

func TestCksumWorkerCksumFileSrcMissing(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	os.WriteFile(filepath.Join(dstDir, "file.txt"), []byte("content"), 0o644)

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewSource(dstDir)

	w := NewCksumWorker(0, src, dst, nil, 0, model.CksumMD5, false)
	result := w.cksumOne(model.DiscoverItem{RelPath: "file.txt", FileType: model.FileRegular, FileSize: 7})

	if result.CksumStatus != model.CksumError {
		t.Errorf("expected CksumError for missing src file, got %v", result.CksumStatus)
	}
	if result.Err == nil {
		t.Error("expected error for missing src file")
	}
}

func TestCksumWorkerSkipByMtime(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	os.WriteFile(filepath.Join(srcDir, "file.txt"), []byte("content"), 0o644)
	os.WriteFile(filepath.Join(dstDir, "file.txt"), []byte("content"), 0o644)
	// Ensure identical mtime for skip-by-mtime to trigger
	now := time.Now().Truncate(time.Second)
	os.Chtimes(filepath.Join(srcDir, "file.txt"), now, now)
	os.Chtimes(filepath.Join(dstDir, "file.txt"), now, now)

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewSource(dstDir)

	w := NewCksumWorker(0, src, dst, nil, 0, model.CksumMD5, true)
	result := w.cksumOne(model.DiscoverItem{RelPath: "file.txt", FileType: model.FileRegular, FileSize: 7, Mtime: now.UnixNano()})

	if result.CksumStatus != model.CksumPass {
		t.Errorf("expected CksumPass when skip-by-mtime matches, got %v", result.CksumStatus)
	}
	if !result.Skipped {
		t.Error("expected Skipped=true for skip-by-mtime")
	}
}
