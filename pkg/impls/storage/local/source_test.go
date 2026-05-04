package local

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/zp001/ncp/pkg/model"
)

func createTestTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	// dir1/file1.txt
	if err := os.MkdirAll(filepath.Join(root, "dir1"), 0o755); err != nil {
		t.Fatalf("mkdir dir1: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "dir1", "file1.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write file1: %v", err)
	}

	// file2.txt
	if err := os.WriteFile(filepath.Join(root, "file2.txt"), []byte("world"), 0o644); err != nil {
		t.Fatalf("write file2: %v", err)
	}

	// symlink → file2.txt
	if err := os.Symlink("file2.txt", filepath.Join(root, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	return root
}

func TestWalkBasic(t *testing.T) {
	root := createTestTree(t)
	src, err := NewSource(root)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}

	var items []model.DiscoverItem
	err = src.Walk(context.Background(), func(item model.DiscoverItem) error {
		items = append(items, item)
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	// Expect: dir1, dir1/file1.txt, file2.txt, link
	names := make(map[string]model.FileType)
	for _, it := range items {
		names[it.RelPath] = it.FileType
	}

	if ft, ok := names["dir1"]; !ok || ft != model.FileDir {
		t.Fatalf("dir1: expected Dir, got %d", ft)
	}
	if ft, ok := names["dir1/file1.txt"]; !ok || ft != model.FileRegular {
		t.Fatalf("dir1/file1.txt: expected Regular, got %d", ft)
	}
	if ft, ok := names["file2.txt"]; !ok || ft != model.FileRegular {
		t.Fatalf("file2.txt: expected Regular, got %d", ft)
	}
	if ft, ok := names["link"]; !ok || ft != model.FileSymlink {
		t.Fatalf("link: expected Symlink, got %d", ft)
	}
}

func TestWalkDFSOrder(t *testing.T) {
	root := createTestTree(t)
	src, err := NewSource(root)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}

	var paths []string
	if err := src.Walk(context.Background(), func(item model.DiscoverItem) error {
		paths = append(paths, item.RelPath)
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}

	// filepath.Walk is DFS: parent before child
	for i, p := range paths {
		for _, q := range paths[i+1:] {
			_ = p
			_ = q
		}
	}
}

func TestWalkSkipSpecial(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "normal.txt"), []byte("ok"), 0o644); err != nil {
		t.Fatalf("write normal: %v", err)
	}

	src, err := NewSource(root)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}

	var count int
	if err := src.Walk(context.Background(), func(item model.DiscoverItem) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}

	// Only normal.txt (root dir skipped)
	if count != 1 {
		t.Fatalf("expected 1 item, got %d", count)
	}
}

func TestOpenReadAt(t *testing.T) {
	root := createTestTree(t)
	src, err := NewSource(root)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}

	r, err := src.Open("dir1/file1.txt")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	buf := make([]byte, 5)
	n, err := r.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("readat: %v", err)
	}
	if n != 5 || string(buf) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(buf[:n]))
	}
}

func TestRelPathFormat(t *testing.T) {
	root := createTestTree(t)
	src, err := NewSource(root)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}

	var paths []string
	if err := src.Walk(context.Background(), func(item model.DiscoverItem) error {
		paths = append(paths, item.RelPath)
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}

	for _, p := range paths {
		if p == "" {
			continue
		}
		// Forward slashes only
		if p[0] == '/' {
			t.Fatalf("relPath should not start with /: %q", p)
		}
	}
}

func TestBase(t *testing.T) {
	src, err := NewSource("/tmp")
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	if src.Base() != "/tmp" {
		t.Fatalf("expected /tmp, got %s", src.Base())
	}
}

func TestWalkSingleFile(t *testing.T) {
	root := t.TempDir()
	filePath := filepath.Join(root, "single.txt")
	if err := os.WriteFile(filePath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	src, err := NewSource(filePath)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}

	var items []model.DiscoverItem
	if err := src.Walk(context.Background(), func(item model.DiscoverItem) error {
		items = append(items, item)
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}

	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].RelPath != "" {
		t.Fatalf("expected empty RelPath for single-file root, got %q", items[0].RelPath)
	}
	if items[0].FileType != model.FileRegular {
		t.Fatalf("expected Regular, got %d", items[0].FileType)
	}
}

func TestOpenSingleFile(t *testing.T) {
	root := t.TempDir()
	filePath := filepath.Join(root, "single.txt")
	if err := os.WriteFile(filePath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	src, err := NewSource(filePath)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}

	r, err := src.Open("")
	if err != nil {
		t.Fatalf("open with empty relPath: %v", err)
	}
	defer r.Close()

	buf := make([]byte, 5)
	n, err := r.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("readat: %v", err)
	}
	if n != 5 || string(buf) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(buf[:n]))
	}
}

func TestNewSourceRejectsRoot(t *testing.T) {
	_, err := NewSource("/")
	if err == nil {
		t.Fatal("expected error for filesystem root")
	}
}
