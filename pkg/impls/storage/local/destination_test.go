package local

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/zp001/ncp/pkg/model"
)

func TestDestinationMkdir(t *testing.T) {
	dst, err := NewDestination(t.TempDir())
	if err != nil {
		t.Fatalf("new destination: %v", err)
	}

	if err := dst.Mkdir(context.Background(), "a/b/c", 0o755, 0, 0); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	st, err := os.Stat(filepath.Join(dst.Base(), "a", "b", "c"))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if !st.IsDir() {
		t.Fatal("expected directory")
	}
}

func TestDestinationOpenFileWriteAt(t *testing.T) {
	dst, err := NewDestination(t.TempDir())
	if err != nil {
		t.Fatalf("new destination: %v", err)
	}

	w, err := dst.OpenFile(context.Background(), "dir/file.txt", 5, 0o644, 0, 0)
	if err != nil {
		t.Fatalf("openfile: %v", err)
	}

	n, err := w.WriteAt([]byte("hello"), 0)
	if err != nil {
		t.Fatalf("writeat: %v", err)
	}
	if n != 5 {
		t.Fatalf("expected 5 bytes written, got %d", n)
	}

	// Close with nil checksum (local copy ignores it)
	if err := w.Close(context.Background(), nil); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dst.Base(), "dir", "file.txt"))
	if err != nil {
		t.Fatalf("readfile: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(data))
	}
}

func TestDestinationSymlink(t *testing.T) {
	dst, err := NewDestination(t.TempDir())
	if err != nil {
		t.Fatalf("new destination: %v", err)
	}

	// Create target first
	if err := os.WriteFile(filepath.Join(dst.Base(), "target.txt"), []byte("data"), 0o644); err != nil {
		t.Fatalf("write target: %v", err)
	}

	if err := dst.Symlink(context.Background(), "link", "target.txt"); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	target, err := os.Readlink(filepath.Join(dst.Base(), "link"))
	if err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if target != "target.txt" {
		t.Fatalf("expected 'target.txt', got %q", target)
	}
}

func TestDestinationSetMetadataChmod(t *testing.T) {
	dst, err := NewDestination(t.TempDir())
	if err != nil {
		t.Fatalf("new destination: %v", err)
	}

	// Create a file first
	w, err := dst.OpenFile(context.Background(), "meta.txt", 0, 0o644, 0, 0)
	if err != nil {
		t.Fatalf("openfile: %v", err)
	}
	if _, err := w.WriteAt([]byte("x"), 0); err != nil {
		t.Fatalf("writeat: %v", err)
	}
	if err := w.Close(context.Background(), nil); err != nil {
		t.Fatalf("close: %v", err)
	}

	meta := model.FileMetadata{Mode: 0o755}
	if err := dst.SetMetadata(context.Background(), "meta.txt", meta); err != nil {
		t.Fatalf("set metadata: %v", err)
	}

	st, err := os.Stat(filepath.Join(dst.Base(), "meta.txt"))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if st.Mode().Perm() != 0o755 {
		t.Fatalf("expected mode 0755, got %04o", st.Mode().Perm())
	}
}

func TestDestinationAutoMkdir(t *testing.T) {
	dst, err := NewDestination(t.TempDir())
	if err != nil {
		t.Fatalf("new destination: %v", err)
	}

	// OpenFile with nested dirs should auto-create parents
	w, err := dst.OpenFile(context.Background(), "deep/nested/dir/file.txt", 0, 0o644, 0, 0)
	if err != nil {
		t.Fatalf("openfile with nested dirs: %v", err)
	}
	if err := w.Close(context.Background(), nil); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dst.Base(), "deep", "nested", "dir")); err != nil {
		t.Fatalf("parent dir should exist: %v", err)
	}
}

func TestWriterSync(t *testing.T) {
	dst, err := NewDestination(t.TempDir())
	if err != nil {
		t.Fatalf("new destination: %v", err)
	}

	w, err := dst.OpenFile(context.Background(), "sync.txt", 0, 0o644, 0, 0)
	if err != nil {
		t.Fatalf("openfile: %v", err)
	}
	if _, err := w.WriteAt([]byte("data"), 0); err != nil {
		t.Fatalf("writeat: %v", err)
	}

	if err := w.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	if err := w.Close(context.Background(), nil); err != nil {
		t.Fatalf("close: %v", err)
	}
}
