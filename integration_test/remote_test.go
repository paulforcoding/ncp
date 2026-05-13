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

	ctx := context.Background()
	if err := src.BeginTask(ctx, "test-task"); err != nil {
		t.Fatalf("begin task: %v", err)
	}
	defer src.EndTask(ctx, storage.TaskSummary{})

	var items []storage.DiscoverItem
	err = src.Walk(ctx, func(_ context.Context, item storage.DiscoverItem) error {
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

	ctx := context.Background()
	if err := src.BeginTask(ctx, "test-task"); err != nil {
		t.Fatalf("begin task: %v", err)
	}
	defer src.EndTask(ctx, storage.TaskSummary{})

	r, err := src.Open(ctx, "data.bin")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close(ctx)

	buf := make([]byte, len(content))
	n, err := r.Read(ctx, buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n != len(content) || string(buf) != string(content) {
		t.Fatalf("content mismatch: got %q (%d bytes)", string(buf[:n]), n)
	}

	// After full read, next read should return EOF
	partial := make([]byte, 5)
	_, err = r.Read(ctx, partial)
	if err == nil {
		t.Fatal("expected EOF after full read")
	}
}

// TestIntegration_RemotePull_Stat verifies Source.Stat returns correct metadata.
func TestIntegration_RemotePull_Stat(t *testing.T) {
	serveDir := t.TempDir()
	os.WriteFile(filepath.Join(serveDir, "statme.txt"), []byte("stat content"), 0o644)

	addr := startTestServer(t, serveDir)

	src, err := remote.NewSource(addr, serveDir)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}

	ctx := context.Background()
	if err := src.BeginTask(ctx, "test-task"); err != nil {
		t.Fatalf("begin task: %v", err)
	}
	defer src.EndTask(ctx, storage.TaskSummary{})

	item, err := src.Stat(ctx, "statme.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if item.RelPath != "statme.txt" {
		t.Fatalf("relpath mismatch: got %q", item.RelPath)
	}
	if item.FileType != model.FileRegular {
		t.Fatalf("filetype: expected Regular, got %d", item.FileType)
	}
	if item.Size != 12 {
		t.Fatalf("size: expected 12, got %d", item.Size)
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

	srcFactory := func(id int) (storage.Source, error) {
		return remote.NewSource(addr, serveDir)
	}

	store := openTestStore(t)
	job := copy.NewJob(srcObj, dstObj, store,
		copy.WithParallelism(1),
		copy.WithSrcFactory(srcFactory),
		copy.WithEnsureDirMtime(false),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srcObj.BeginTask(ctx, job.TaskID()); err != nil {
		t.Fatalf("begin task: %v", err)
	}
	defer srcObj.EndTask(ctx, storage.TaskSummary{})

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

	srcFactory := func(id int) (storage.Source, error) {
		return remote.NewSource(addr, serveDir)
	}

	store := openTestStore(t)
	job := copy.NewJob(srcObj, dstObj, store,
		copy.WithParallelism(4),
		copy.WithSrcFactory(srcFactory),
		copy.WithEnsureDirMtime(false),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := srcObj.BeginTask(ctx, job.TaskID()); err != nil {
		t.Fatalf("begin task: %v", err)
	}
	defer srcObj.EndTask(ctx, storage.TaskSummary{})

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
	pushServeDir := t.TempDir()
	pullServeDir := t.TempDir()
	dst := t.TempDir()

	CreateBasicTestTree(t, src)

	pushAddr := startTestServer(t, pushServeDir)

	// Phase 1: Push
	srcObj, _ := local.NewSource(src)
	pushStore := openTestStore(t)
	dstFactory := func(id int) (storage.Destination, error) {
		return remote.NewDestination(pushAddr, pushServeDir)
	}

	pushJob := copy.NewJob(srcObj, nil, pushStore,
		copy.WithParallelism(2),
		copy.WithDstFactory(dstFactory),
		copy.WithEnsureDirMtime(false),
		copy.WithDstBase("ncp://"+pushAddr),
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

	// Copy push results to pull serve dir for Phase 2
	filepath.Walk(pushServeDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(pushServeDir, path)
		if rel == "." {
			return nil
		}
		dstPath := filepath.Join(pullServeDir, rel)
		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			return os.Symlink(target, dstPath)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(dstPath, data, info.Mode())
	})

	// Phase 2: Pull (from a new source server)
	pullAddr := startTestServer(t, pullServeDir)
	pullSrc, _ := remote.NewSource(pullAddr, pullServeDir)
	pullDst, _ := local.NewDestination(dst)
	pullStore := openTestStore(t)

	pullSrcFactory := func(id int) (storage.Source, error) {
		return remote.NewSource(pullAddr, pullServeDir)
	}

	pullJob := copy.NewJob(pullSrc, pullDst, pullStore,
		copy.WithParallelism(2),
		copy.WithSrcFactory(pullSrcFactory),
		copy.WithEnsureDirMtime(false),
	)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()

	if err := pullSrc.BeginTask(ctx2, pullJob.TaskID()); err != nil {
		t.Fatalf("begin task: %v", err)
	}
	defer pullSrc.EndTask(ctx2, storage.TaskSummary{})

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

	srcFactory := func(id int) (storage.Source, error) {
		return remote.NewSource(addr, serveDir)
	}

	store := openTestStore(t)
	job := copy.NewJob(srcObj, dstObj, store,
		copy.WithParallelism(1),
		copy.WithSrcFactory(srcFactory),
		copy.WithEnsureDirMtime(false),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := srcObj.BeginTask(ctx, job.TaskID()); err != nil {
		t.Fatalf("begin task: %v", err)
	}
	defer srcObj.EndTask(ctx, storage.TaskSummary{})

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

	srcFactory := func(id int) (storage.Source, error) {
		return remote.NewSource(addr, serveDir)
	}

	store := openTestStore(t)
	job := copy.NewJob(srcObj, dstObj, store,
		copy.WithParallelism(1),
		copy.WithSrcFactory(srcFactory),
		copy.WithEnsureDirMtime(false),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srcObj.BeginTask(ctx, job.TaskID()); err != nil {
		t.Fatalf("begin task: %v", err)
	}
	defer srcObj.EndTask(ctx, storage.TaskSummary{})

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

	srcFactory := func(id int) (storage.Source, error) {
		return remote.NewSource(addr, serveDir)
	}

	store := openTestStore(t)
	job := copy.NewJob(srcObj, dstObj, store,
		copy.WithParallelism(1),
		copy.WithSrcFactory(srcFactory),
		copy.WithEnsureDirMtime(false),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srcObj.BeginTask(ctx, job.TaskID()); err != nil {
		t.Fatalf("begin task: %v", err)
	}
	defer srcObj.EndTask(ctx, storage.TaskSummary{})

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

	srcFactory := func(id int) (storage.Source, error) {
		return remote.NewSource(addr, serveDir)
	}

	store := openTestStore(t)
	job := copy.NewJob(srcObj, dstObj, store,
		copy.WithParallelism(1),
		copy.WithSrcFactory(srcFactory),
		copy.WithEnsureDirMtime(false),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srcObj.BeginTask(ctx, job.TaskID()); err != nil {
		t.Fatalf("begin task: %v", err)
	}
	defer srcObj.EndTask(ctx, storage.TaskSummary{})

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

func (d *failAfterNOpen) OpenFile(ctx context.Context, relPath string, size int64, mode os.FileMode, uid, gid int) (storage.FileWriter, error) {
	d.mu.Lock()
	d.count++
	if d.count > d.failAt {
		d.mu.Unlock()
		return nil, fmt.Errorf("simulated error after %d files", d.failAt)
	}
	d.mu.Unlock()
	return d.Destination.OpenFile(ctx, relPath, size, mode, uid, gid)
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

	srcFactory := func(id int) (storage.Source, error) {
		return remote.NewSource(addr, serveDir)
	}

	store := openTestStore(t)

	const failAt = 30
	failDst := &failAfterNOpen{Destination: realDst, failAt: failAt}

	ctx := context.Background()

	job := copy.NewJob(srcObj, failDst, store,
		copy.WithParallelism(2),
		copy.WithSrcFactory(srcFactory),
		copy.WithEnsureDirMtime(false),
	)

	if err := srcObj.BeginTask(ctx, job.TaskID()); err != nil {
		t.Fatalf("begin task: %v", err)
	}

	exitCode, err := job.Run(ctx)
	if exitCode != 2 {
		t.Fatalf("expected exit code 2, got %d", exitCode)
	}
	if err == nil {
		t.Fatal("expected error for partial failure")
	}

	// Resume with real destination
	job2 := copy.NewJob(srcObj, realDst, store,
		copy.WithResume(true),
		copy.WithTaskID(job.TaskID()),
		copy.WithSrcFactory(srcFactory),
		copy.WithEnsureDirMtime(false),
	)

	exitCode, err = job2.Run(ctx)
	if err != nil {
		t.Fatalf("resume job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0 on resume, got %d", exitCode)
	}

	srcObj.EndTask(ctx, storage.TaskSummary{})

	if err := VerifyCopy(serveDir, dst); err != nil {
		t.Fatalf("verify copy after resume: %v", err)
	}
}

// --- Remote → Remote copy ---

// TestIntegration_RemoteToRemote_Copy covers matrix case #13 (Remote→Remote, copy, no-resume).
func TestIntegration_RemoteToRemote_Copy(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	os.MkdirAll(srcDir, 0o755)
	os.MkdirAll(dstDir, 0o755)

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	for relPath, content := range files {
		path := filepath.Join(srcDir, relPath)
		os.MkdirAll(filepath.Dir(path), 0o755)
		os.WriteFile(path, []byte(content), 0o644)
	}

	srcAddr := startTestServer(t, srcDir)
	dstAddr := startTestServer(t, dstDir)

	srcObj, _ := remote.NewSource(srcAddr, srcDir)
	store := openTestStore(t)

	srcFactory := func(id int) (storage.Source, error) {
		return remote.NewSource(srcAddr, srcDir)
	}
	dstFactory := func(id int) (storage.Destination, error) {
		return remote.NewDestination(dstAddr, dstDir)
	}

	job := copy.NewJob(srcObj, nil, store,
		copy.WithParallelism(2),
		copy.WithSrcFactory(srcFactory),
		copy.WithDstFactory(dstFactory),
		copy.WithEnsureDirMtime(false),
		copy.WithDstBase("ncp://"+dstAddr),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srcObj.BeginTask(ctx, job.TaskID()); err != nil {
		t.Fatalf("begin task: %v", err)
	}
	defer srcObj.EndTask(ctx, storage.TaskSummary{})

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	for relPath, want := range files {
		path := filepath.Join(dstDir, relPath)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", relPath, err)
		}
		if string(data) != want {
			t.Errorf("content mismatch %s: got %q, want %q", relPath, string(data), want)
		}
	}
}

// TestIntegration_RemoteToRemote_Copy_Resume covers matrix case #14 (Remote→Remote, copy, resume).
func TestIntegration_RemoteToRemote_Copy_Resume(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	os.MkdirAll(srcDir, 0o755)
	os.MkdirAll(dstDir, 0o755)

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
		"c.txt":        "gamma",
	}
	for relPath, content := range files {
		path := filepath.Join(srcDir, relPath)
		os.MkdirAll(filepath.Dir(path), 0o755)
		os.WriteFile(path, []byte(content), 0o644)
	}

	srcAddr := startTestServer(t, srcDir)
	dstAddr := startTestServer(t, dstDir)

	srcObj, _ := remote.NewSource(srcAddr, srcDir)
	store := openTestStore(t)

	srcFactory := func(id int) (storage.Source, error) {
		return remote.NewSource(srcAddr, srcDir)
	}

	mu := &sync.Mutex{}
	count := 0
	dstFactory := func(id int) (storage.Destination, error) {
		dst, err := remote.NewDestination(dstAddr, dstDir)
		if err != nil {
			return nil, err
		}
		return &failAfterNShared{Destination: dst, mu: mu, count: &count, failAt: 1}, nil
	}

	ctx := context.Background()

	job := copy.NewJob(srcObj, nil, store,
		copy.WithParallelism(2),
		copy.WithSrcFactory(srcFactory),
		copy.WithDstFactory(dstFactory),
		copy.WithEnsureDirMtime(false),
		copy.WithDstBase("ncp://"+dstAddr),
	)

	if err := srcObj.BeginTask(ctx, job.TaskID()); err != nil {
		t.Fatalf("begin task: %v", err)
	}

	exitCode, err := job.Run(ctx)
	if exitCode != 2 {
		t.Fatalf("expected exit code 2, got %d", exitCode)
	}
	if err == nil {
		t.Fatal("expected error for partial failure")
	}

	dstFactory2 := func(id int) (storage.Destination, error) {
		return remote.NewDestination(dstAddr, dstDir)
	}

	job2 := copy.NewJob(srcObj, nil, store,
		copy.WithResume(true),
		copy.WithTaskID(job.TaskID()),
		copy.WithSrcFactory(srcFactory),
		copy.WithDstFactory(dstFactory2),
		copy.WithEnsureDirMtime(false),
		copy.WithDstBase("ncp://"+dstAddr),
	)

	exitCode, err = job2.Run(ctx)
	if err != nil {
		t.Fatalf("resume job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0 on resume, got %d", exitCode)
	}

	srcObj.EndTask(ctx, storage.TaskSummary{})

	for relPath, want := range files {
		path := filepath.Join(dstDir, relPath)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", relPath, err)
		}
		if string(data) != want {
			t.Errorf("content mismatch %s: got %q, want %q", relPath, string(data), want)
		}
	}
}
