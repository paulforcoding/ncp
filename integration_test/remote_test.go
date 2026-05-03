//go:build integration

package integration

import (
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/pkg/impls/storage/local"
	"github.com/zp001/ncp/pkg/impls/storage/remote"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// --- Push tests (ncp:// as destination) ---

// TestIntegration_RemotePush_Basic pushes files+dirs+symlinks to ncp:// server.
func TestIntegration_RemotePush_Basic(t *testing.T) {
	src := t.TempDir()
	serveDir := t.TempDir()

	os.MkdirAll(filepath.Join(src, "subdir"), 0o755)
	os.WriteFile(filepath.Join(src, "file1.txt"), []byte("hello"), 0o644)
	os.WriteFile(filepath.Join(src, "subdir", "file2.txt"), []byte("world"), 0o644)
	os.Symlink("file1.txt", filepath.Join(src, "link1"))

	addr := startTestServer(t, serveDir)

	srcObj, err := local.NewSource(src)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}

	store := openTestStore(t)

	dstFactory := func(id int) (storage.Destination, error) {
		return remote.NewDestination(addr, serveDir)
	}

	job := copy.NewJob(srcObj, nil, store,
		copy.WithParallelism(1),
		copy.WithDstFactory(dstFactory),
		copy.WithEnsureDirMtime(false),
		copy.WithDstBase("ncp://"+addr),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	data, err := os.ReadFile(filepath.Join(serveDir, "file1.txt"))
	if err != nil || string(data) != "hello" {
		t.Fatalf("file1.txt content mismatch: %q, err %v", string(data), err)
	}
	data, err = os.ReadFile(filepath.Join(serveDir, "subdir", "file2.txt"))
	if err != nil || string(data) != "world" {
		t.Fatalf("file2.txt content mismatch: %q, err %v", string(data), err)
	}

	target, err := os.Readlink(filepath.Join(serveDir, "link1"))
	if err != nil || target != "file1.txt" {
		t.Fatalf("symlink target mismatch: got %q, err %v", target, err)
	}

	if _, err := os.Stat(filepath.Join(serveDir, "subdir")); err != nil {
		t.Fatalf("subdir missing: %v", err)
	}
}

// TestIntegration_RemotePush_Parallel pushes 200 files with CopyParallelism=4.
func TestIntegration_RemotePush_Parallel(t *testing.T) {
	src := t.TempDir()
	serveDir := t.TempDir()

	if err := CreateTestTree(src, 200); err != nil {
		t.Fatalf("create test tree: %v", err)
	}

	addr := startTestServer(t, serveDir)

	srcObj, _ := local.NewSource(src)
	store := openTestStore(t)

	dstFactory := func(id int) (storage.Destination, error) {
		return remote.NewDestination(addr, serveDir)
	}

	job := copy.NewJob(srcObj, nil, store,
		copy.WithParallelism(4),
		copy.WithDstFactory(dstFactory),
		copy.WithEnsureDirMtime(false),
		copy.WithDstBase("ncp://"+addr),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	if err := VerifyCopy(src, serveDir); err != nil {
		t.Fatalf("verify copy: %v", err)
	}
}

// --- Pull tests (ncp:// as source) ---

// TestIntegration_RemotePull_Walk verifies Source.Walk returns the complete directory tree.
func TestIntegration_RemotePull_Walk(t *testing.T) {
	serveDir := t.TempDir()
	CreateBasicTestTree(t, serveDir)

	addr := startTestServer(t, serveDir)

	src, err := remote.NewSource(addr, serveDir)
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

	names := make(map[string]model.FileType)
	for _, it := range items {
		names[it.RelPath] = it.FileType
	}

	if ft, ok := names["subdir"]; !ok || ft != model.FileDir {
		t.Fatalf("subdir: expected Dir, got %d, exists %v", ft, ok)
	}
	if ft, ok := names["file1.txt"]; !ok || ft != model.FileRegular {
		t.Fatalf("file1.txt: expected Regular, got %d, exists %v", ft, ok)
	}
	if ft, ok := names["link1"]; !ok || ft != model.FileSymlink {
		t.Fatalf("link1: expected Symlink, got %d, exists %v", ft, ok)
	}
	if ft, ok := names["empty.txt"]; !ok || ft != model.FileRegular {
		t.Fatalf("empty.txt: expected Regular, got %d, exists %v", ft, ok)
	}
}

// TestIntegration_RemotePull_OpenRead verifies Source.Open + Reader.ReadAt data correctness.
func TestIntegration_RemotePull_OpenRead(t *testing.T) {
	serveDir := t.TempDir()
	content := []byte("hello remote world")
	os.WriteFile(filepath.Join(serveDir, "data.bin"), content, 0o644)

	addr := startTestServer(t, serveDir)

	src, err := remote.NewSource(addr, serveDir)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}

	r, err := src.Open("data.bin")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	buf := make([]byte, len(content))
	n, err := r.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("readat: %v", err)
	}
	if n != len(content) || string(buf) != string(content) {
		t.Fatalf("content mismatch: got %q (%d bytes)", string(buf[:n]), n)
	}

	// Partial read at offset
	partial := make([]byte, 5)
	n, err = r.ReadAt(partial, 6)
	if err != nil {
		t.Fatalf("partial readat: %v", err)
	}
	if n != 5 || string(partial) != "remot" {
		t.Fatalf("partial mismatch: got %q", string(partial[:n]))
	}
}

// TestIntegration_RemotePull_Restat verifies Source.Restat returns correct metadata.
func TestIntegration_RemotePull_Restat(t *testing.T) {
	serveDir := t.TempDir()
	os.WriteFile(filepath.Join(serveDir, "statme.txt"), []byte("stat content"), 0o644)

	addr := startTestServer(t, serveDir)

	src, err := remote.NewSource(addr, serveDir)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}

	item, err := src.Restat("statme.txt")
	if err != nil {
		t.Fatalf("restat: %v", err)
	}
	if item.RelPath != "statme.txt" {
		t.Fatalf("relpath mismatch: got %q", item.RelPath)
	}
	if item.FileType != model.FileRegular {
		t.Fatalf("filetype: expected Regular, got %d", item.FileType)
	}
	if item.FileSize != 12 {
		t.Fatalf("size: expected 12, got %d", item.FileSize)
	}
}

// TestIntegration_RemotePull_BasicCopy pulls files from ncp:// to local via copy.NewJob.
func TestIntegration_RemotePull_BasicCopy(t *testing.T) {
	serveDir := t.TempDir()
	dst := t.TempDir()

	CreateBasicTestTree(t, serveDir)

	addr := startTestServer(t, serveDir)

	srcObj, err := remote.NewSource(addr, serveDir)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}

	dstObj, err := local.NewDestination(dst)
	if err != nil {
		t.Fatalf("new destination: %v", err)
	}

	store := openTestStore(t)
	job := copy.NewJob(srcObj, dstObj, store,
		copy.WithParallelism(1),
		copy.WithEnsureDirMtime(false),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("pull job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	if err := VerifyCopy(serveDir, dst); err != nil {
		t.Fatalf("verify copy: %v", err)
	}
}

// TestIntegration_RemotePull_ParallelCopy pulls with CopyParallelism=4.
func TestIntegration_RemotePull_ParallelCopy(t *testing.T) {
	serveDir := t.TempDir()
	dst := t.TempDir()

	if err := CreateTestTree(serveDir, 200); err != nil {
		t.Fatalf("create test tree: %v", err)
	}

	addr := startTestServer(t, serveDir)

	srcObj, _ := remote.NewSource(addr, serveDir)
	dstObj, _ := local.NewDestination(dst)

	store := openTestStore(t)
	job := copy.NewJob(srcObj, dstObj, store,
		copy.WithParallelism(4),
		copy.WithEnsureDirMtime(false),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("pull job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	if err := VerifyCopy(serveDir, dst); err != nil {
		t.Fatalf("verify copy: %v", err)
	}
}

// TestIntegration_RemoteRoundTrip pushes local→ncp:// then pulls ncp://→local2, verifies both match.
func TestIntegration_RemoteRoundTrip(t *testing.T) {
	src := t.TempDir()
	serveDir := t.TempDir()
	dst := t.TempDir()

	CreateBasicTestTree(t, src)

	addr := startTestServer(t, serveDir)

	// Phase 1: Push
	srcObj, _ := local.NewSource(src)
	pushStore := openTestStore(t)
	dstFactory := func(id int) (storage.Destination, error) {
		return remote.NewDestination(addr, serveDir)
	}

	pushJob := copy.NewJob(srcObj, nil, pushStore,
		copy.WithParallelism(2),
		copy.WithDstFactory(dstFactory),
		copy.WithEnsureDirMtime(false),
		copy.WithDstBase("ncp://"+addr),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	exitCode, err := pushJob.Run(ctx)
	if err != nil {
		t.Fatalf("push job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("push exit code 0 expected, got %d", exitCode)
	}

	// Phase 2: Pull
	pullSrc, _ := remote.NewSource(addr, serveDir)
	pullDst, _ := local.NewDestination(dst)
	pullStore := openTestStore(t)

	pullJob := copy.NewJob(pullSrc, pullDst, pullStore,
		copy.WithParallelism(2),
		copy.WithEnsureDirMtime(false),
	)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()

	exitCode, err = pullJob.Run(ctx2)
	if err != nil {
		t.Fatalf("pull job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("pull exit code 0 expected, got %d", exitCode)
	}

	// Verify round-trip: src == dst
	if err := VerifyCopy(src, dst); err != nil {
		t.Fatalf("round-trip verify: %v", err)
	}
}

// TestIntegration_RemotePull_LargeFile verifies multi-MB file md5 match.
func TestIntegration_RemotePull_LargeFile(t *testing.T) {
	serveDir := t.TempDir()
	dst := t.TempDir()

	// Create 4MB file with known pattern
	size := 4 << 20
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}
	wantMD5 := fmt.Sprintf("%x", md5.Sum(data))
	os.WriteFile(filepath.Join(serveDir, "large.bin"), data, 0o644)

	addr := startTestServer(t, serveDir)

	srcObj, _ := remote.NewSource(addr, serveDir)
	dstObj, _ := local.NewDestination(dst)

	store := openTestStore(t)
	job := copy.NewJob(srcObj, dstObj, store,
		copy.WithParallelism(1),
		copy.WithEnsureDirMtime(false),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("pull job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	gotData, err := os.ReadFile(filepath.Join(dst, "large.bin"))
	if err != nil {
		t.Fatalf("readfile: %v", err)
	}
	gotMD5 := fmt.Sprintf("%x", md5.Sum(gotData))
	if gotMD5 != wantMD5 {
		t.Fatalf("MD5 mismatch: got %s, want %s", gotMD5, wantMD5)
	}
}

// TestIntegration_RemotePull_EmptyFile verifies empty file is pulled correctly.
func TestIntegration_RemotePull_EmptyFile(t *testing.T) {
	serveDir := t.TempDir()
	dst := t.TempDir()

	os.WriteFile(filepath.Join(serveDir, "empty.txt"), []byte{}, 0o644)

	addr := startTestServer(t, serveDir)

	srcObj, _ := remote.NewSource(addr, serveDir)
	dstObj, _ := local.NewDestination(dst)

	store := openTestStore(t)
	job := copy.NewJob(srcObj, dstObj, store,
		copy.WithParallelism(1),
		copy.WithEnsureDirMtime(false),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("pull job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	info, err := os.Stat(filepath.Join(dst, "empty.txt"))
	if err != nil {
		t.Fatalf("stat empty.txt: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("expected 0 bytes, got %d", info.Size())
	}
}

// TestIntegration_RemotePull_Symlink verifies symlink target is preserved.
func TestIntegration_RemotePull_Symlink(t *testing.T) {
	serveDir := t.TempDir()
	dst := t.TempDir()

	os.WriteFile(filepath.Join(serveDir, "target.txt"), []byte("data"), 0o644)
	os.Symlink("target.txt", filepath.Join(serveDir, "link"))

	addr := startTestServer(t, serveDir)

	srcObj, _ := remote.NewSource(addr, serveDir)
	dstObj, _ := local.NewDestination(dst)

	store := openTestStore(t)
	job := copy.NewJob(srcObj, dstObj, store,
		copy.WithParallelism(1),
		copy.WithEnsureDirMtime(false),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("pull job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	target, err := os.Readlink(filepath.Join(dst, "link"))
	if err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if target != "target.txt" {
		t.Fatalf("symlink target mismatch: got %q, want %q", target, "target.txt")
	}
}

// TestIntegration_RemotePull_ChinesePath verifies Chinese/UTF-8 paths work.
func TestIntegration_RemotePull_ChinesePath(t *testing.T) {
	serveDir := t.TempDir()
	dst := t.TempDir()

	os.MkdirAll(filepath.Join(serveDir, "中文目录"), 0o755)
	os.WriteFile(filepath.Join(serveDir, "中文目录", "文件.txt"), []byte("中文内容"), 0o644)

	addr := startTestServer(t, serveDir)

	srcObj, _ := remote.NewSource(addr, serveDir)
	dstObj, _ := local.NewDestination(dst)

	store := openTestStore(t)
	job := copy.NewJob(srcObj, dstObj, store,
		copy.WithParallelism(1),
		copy.WithEnsureDirMtime(false),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("pull job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	data, err := os.ReadFile(filepath.Join(dst, "中文目录", "文件.txt"))
	if err != nil {
		t.Fatalf("readfile: %v", err)
	}
	if string(data) != "中文内容" {
		t.Fatalf("content mismatch: got %q", string(data))
	}
}

// failAfterNOpen wraps a Destination and fails OpenFile after N successful calls.
type failAfterNOpen struct {
	storage.Destination
	mu     sync.Mutex
	count  int
	failAt int
}

func (d *failAfterNOpen) OpenFile(relPath string, size int64, mode os.FileMode, uid, gid int) (storage.Writer, error) {
	d.mu.Lock()
	d.count++
	if d.count > d.failAt {
		d.mu.Unlock()
		return nil, fmt.Errorf("simulated error after %d files", d.failAt)
	}
	d.mu.Unlock()
	return d.Destination.OpenFile(relPath, size, mode, uid, gid)
}

// TestIntegration_RemotePull_Resume verifies pull resume after partial failure.
func TestIntegration_RemotePull_Resume(t *testing.T) {
	serveDir := t.TempDir()
	dst := t.TempDir()

	if err := CreateTestTree(serveDir, 100); err != nil {
		t.Fatalf("create test tree: %v", err)
	}

	addr := startTestServer(t, serveDir)

	srcObj, _ := remote.NewSource(addr, serveDir)
	realDst, _ := local.NewDestination(dst)

	store := openTestStore(t)

	const failAt = 30
	failDst := &failAfterNOpen{Destination: realDst, failAt: failAt}

	job := copy.NewJob(srcObj, failDst, store,
		copy.WithParallelism(2),
		copy.WithEnsureDirMtime(false),
	)

	exitCode, err := job.Run(context.Background())
	if exitCode != 2 {
		t.Fatalf("expected exit code 2, got %d", exitCode)
	}
	if err == nil {
		t.Fatal("expected error for partial failure")
	}

	// Resume with real destination
	job2 := copy.NewJob(srcObj, realDst, store,
		copy.WithResume(true),
		copy.WithEnsureDirMtime(false),
	)

	exitCode, err = job2.Run(context.Background())
	if err != nil {
		t.Fatalf("resume job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0 on resume, got %d", exitCode)
	}

	if err := VerifyCopy(serveDir, dst); err != nil {
		t.Fatalf("verify copy after resume: %v", err)
	}
}
