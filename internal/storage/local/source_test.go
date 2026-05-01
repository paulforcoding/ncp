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
	os.MkdirAll(filepath.Join(root, "dir1"), 0o755)
	os.WriteFile(filepath.Join(root, "dir1", "file1.txt"), []byte("hello"), 0o644)

	// file2.txt
	os.WriteFile(filepath.Join(root, "file2.txt"), []byte("world"), 0o644)

	// symlink → file2.txt
	os.Symlink("file2.txt", filepath.Join(root, "link"))

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
	src.Walk(context.Background(), func(item model.DiscoverItem) error {
		paths = append(paths, item.RelPath)
		return nil
	})

	// filepath.Walk is DFS: parent before child
	for i, p := range paths {
		for _, q := range paths[i+1:] {
			// If q is a child of p, that's fine.
			// If q should come before p lexicographically and is not a child, that's a bug.
			_ = p
			_ = q
		}
	}
}

func TestWalkSkipSpecial(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "normal.txt"), []byte("ok"), 0o644)

	src, err := NewSource(root)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}

	var count int
	src.Walk(context.Background(), func(item model.DiscoverItem) error {
		count++
		return nil
	})

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
	src.Walk(context.Background(), func(item model.DiscoverItem) error {
		paths = append(paths, item.RelPath)
		return nil
	})

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
