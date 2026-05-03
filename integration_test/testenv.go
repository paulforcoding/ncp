//go:build integration

package integration

import (
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/internal/serve"
	"github.com/zp001/ncp/pkg/impls/progress/pebble"
)

// --- Server helpers ---

// startTestServer starts an in-process ncp server on a random port.
// basePath is the server's file root directory. Returns the server address.
// t.Cleanup automatically closes the server.
func startTestServer(t *testing.T, basePath string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := protocol.NewServer(ln, func() protocol.ConnHandler {
		return serve.NewConnHandler()
	})
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return ln.Addr().String()
}

// --- Store helpers ---

// openTestStore creates a pebble store in a temp directory.
// t.Cleanup automatically closes the store.
func openTestStore(t *testing.T) *pebble.Store {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "db")
	s := &pebble.Store{}
	if err := s.Open(dir); err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// --- Fixture helpers (from testutil/fixture.go) ---

// CreateTestTree creates a directory tree under root with the given file count.
func CreateTestTree(root string, fileCount int) error {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}

	dirsPerLevel := 10
	filesPerDir := fileCount / dirsPerLevel
	if filesPerDir < 1 {
		filesPerDir = 1
	}

	created := 0
	for d := 0; d < dirsPerLevel && created < fileCount; d++ {
		dirName := fmt.Sprintf("dir%d", d)
		dirPath := filepath.Join(root, dirName)
		if err := os.MkdirAll(dirPath, 0o755); err != nil {
			return err
		}

		subPath := filepath.Join(dirPath, "sub")
		if err := os.MkdirAll(subPath, 0o755); err != nil {
			return err
		}

		for f := 0; f < filesPerDir && created < fileCount; f++ {
			fileName := fmt.Sprintf("file%d.txt", f)
			filePath := filepath.Join(dirPath, fileName)
			content := fmt.Sprintf("content-%d-%d", d, f)
			if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
				return err
			}
			created++
		}

		for f := 0; f < filesPerDir/2 && created < fileCount; f++ {
			fileName := fmt.Sprintf("subfile%d.txt", f)
			filePath := filepath.Join(subPath, fileName)
			content := fmt.Sprintf("subcontent-%d-%d", d, f)
			if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
				return err
			}
			created++
		}
	}

	targetFile := filepath.Join(root, "dir0", "file0.txt")
	if _, err := os.Stat(targetFile); err == nil {
		if err := os.Symlink(filepath.Join("..", "dir0", "file0.txt"), filepath.Join(root, "link_to_file0")); err != nil {
			return err
		}
	}

	if created < fileCount {
		if err := os.WriteFile(filepath.Join(root, "rootfile.txt"), []byte("root-content"), 0o644); err != nil {
			return err
		}
	}

	return nil
}

// VerifyCopy recursively compares src and dst directories.
func VerifyCopy(src, dst string) error {
	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		dstPath := filepath.Join(dst, rel)

		if info.IsDir() {
			dstInfo, err := os.Stat(dstPath)
			if err != nil {
				return fmt.Errorf("dir missing in dst: %s", rel)
			}
			if !dstInfo.IsDir() {
				return fmt.Errorf("expected dir in dst: %s", rel)
			}
			return nil
		}

		if info.Mode()&fs.ModeSymlink != 0 {
			srcTarget, err := os.Readlink(path)
			if err != nil {
				return err
			}
			dstTarget, err := os.Readlink(dstPath)
			if err != nil {
				return fmt.Errorf("symlink missing in dst: %s", rel)
			}
			if srcTarget != dstTarget {
				return fmt.Errorf("symlink target mismatch %s: src=%s dst=%s", rel, srcTarget, dstTarget)
			}
			return nil
		}

		srcData, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		dstData, err := os.ReadFile(dstPath)
		if err != nil {
			return fmt.Errorf("file missing in dst: %s", rel)
		}
		if string(srcData) != string(dstData) {
			return fmt.Errorf("content mismatch: %s", rel)
		}

		return nil
	})
}

// CountFiles recursively counts files under root.
func CountFiles(root string) (regulars, dirs, symlinks int, err error) {
	err = filepath.Walk(root, func(path string, info fs.FileInfo, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		if rel == "." {
			return nil
		}
		if info.IsDir() {
			dirs++
		} else if info.Mode()&fs.ModeSymlink != 0 {
			symlinks++
		} else {
			regulars++
		}
		return nil
	})
	return
}

// IsRoot returns true if running as root.
func IsRoot() bool {
	return os.Getuid() == 0
}

// MakeUnreadable removes read permission from a file.
func MakeUnreadable(path string) error {
	return os.Chmod(path, 0o000)
}

// MakeReadable restores read permission.
func MakeReadable(path string) error {
	return os.Chmod(path, 0o644)
}

// DirsHaveSameMtime checks that directories in dst have the same mtime as src.
func DirsHaveSameMtime(src, dst string) error {
	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		if err != nil || !info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(src, path)
		if rel == "." {
			return nil
		}
		dstPath := filepath.Join(dst, rel)
		dstInfo, err := os.Stat(dstPath)
		if err != nil {
			return nil
		}
		srcMtime := info.ModTime().Truncate(time.Second)
		dstMtime := dstInfo.ModTime().Truncate(time.Second)
		if !srcMtime.Equal(dstMtime) {
			return fmt.Errorf("mtime mismatch %s: src=%v dst=%v", rel, srcMtime, dstMtime)
		}
		return nil
	})
}

// CreateBasicTestTree creates a simple tree with files, dirs, symlinks, and empty files.
func CreateBasicTestTree(t *testing.T, root string) {
	t.Helper()

	os.MkdirAll(filepath.Join(root, "subdir"), 0o755)
	os.WriteFile(filepath.Join(root, "file1.txt"), []byte("hello"), 0o644)
	os.WriteFile(filepath.Join(root, "subdir", "file2.txt"), []byte("world"), 0o644)
	os.Symlink("file1.txt", filepath.Join(root, "link1"))
	os.WriteFile(filepath.Join(root, "empty.txt"), []byte(""), 0o644)
	os.MkdirAll(filepath.Join(root, "中文目录"), 0o755)
	os.WriteFile(filepath.Join(root, "中文目录", "文件.txt"), []byte("中文内容"), 0o644)
}
