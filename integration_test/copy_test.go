//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/pkg/impls/progress/pebble"
	"github.com/zp001/ncp/pkg/impls/storage/local"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

func runCopyJob(t *testing.T, src, dst string, store *pebble.Store, opts ...copy.JobOption) (int, error) {
	t.Helper()
	srcObj, err := local.NewSource(src)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	dstObj, err := local.NewDestination(dst)
	if err != nil {
		t.Fatalf("new destination: %v", err)
	}
	job := copy.NewJob(srcObj, dstObj, store, opts...)
	ctx := context.Background()
	return job.Run(ctx)
}

// Test 1: Basic copy — regular files, directories, symlinks
func TestIntegration_BasicCopy(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	// Create source tree
	os.MkdirAll(filepath.Join(src, "subdir"), 0o755)
	os.WriteFile(filepath.Join(src, "file1.txt"), []byte("hello"), 0o644)
	os.WriteFile(filepath.Join(src, "subdir", "file2.txt"), []byte("world"), 0o644)
	os.Symlink("file1.txt", filepath.Join(src, "link1"))

	store := openTestStore(t)
	exitCode, err := runCopyJob(t, src, dst, store)
	if err != nil {
		t.Fatalf("copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	// Verify file content
	data, err := os.ReadFile(filepath.Join(dst, "file1.txt"))
	if err != nil || string(data) != "hello" {
		t.Fatalf("file1.txt content mismatch")
	}
	data, err = os.ReadFile(filepath.Join(dst, "subdir", "file2.txt"))
	if err != nil || string(data) != "world" {
		t.Fatalf("file2.txt content mismatch")
	}

	// Verify symlink
	target, err := os.Readlink(filepath.Join(dst, "link1"))
	if err != nil || target != "file1.txt" {
		t.Fatalf("symlink target mismatch: got %q, err %v", target, err)
	}

	// Verify directory
	if _, err := os.Stat(filepath.Join(dst, "subdir")); err != nil {
		t.Fatalf("subdir missing: %v", err)
	}

	// Verify DB
	has, _ := store.HasWalkComplete()
	if !has {
		t.Fatal("expected __walk_complete in DB")
	}
}

// Test 2: Parallel copy — 1000 files with CopyParallelism=4
func TestIntegration_ParallelCopy(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	if err := CreateTestTree(src, 1000); err != nil {
		t.Fatalf("create test tree: %v", err)
	}

	srcRegulars, srcDirs, _, _ := CountFiles(src)
	t.Logf("Source: %d regulars, %d dirs", srcRegulars, srcDirs)

	store := openTestStore(t)
	exitCode, err := runCopyJob(t, src, dst, store, copy.WithParallelism(4))
	if err != nil {
		t.Fatalf("copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	if err := VerifyCopy(src, dst); err != nil {
		t.Fatalf("verify copy: %v", err)
	}
}

// Test 3: Channel full → Walker writes discovered, then dispatches remaining
func TestIntegration_ChannelFullDispatch(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	if err := CreateTestTree(src, 200); err != nil {
		t.Fatalf("create test tree: %v", err)
	}

	store := openTestStore(t)
	exitCode, err := runCopyJob(t, src, dst, store,
		copy.WithParallelism(1),
		copy.WithChannelBuf(1),
	)
	if err != nil {
		t.Fatalf("copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	if err := VerifyCopy(src, dst); err != nil {
		t.Fatalf("verify copy: %v", err)
	}
}

// Test 4: DB progress correctness after copy
func TestIntegration_DBProgressCorrectness(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	os.MkdirAll(filepath.Join(src, "a"), 0o755)
	os.WriteFile(filepath.Join(src, "a", "f1.txt"), []byte("1"), 0o644)
	os.WriteFile(filepath.Join(src, "a", "f2.txt"), []byte("2"), 0o644)
	os.MkdirAll(filepath.Join(src, "b"), 0o755)
	os.WriteFile(filepath.Join(src, "b", "f3.txt"), []byte("3"), 0o644)

	store := openTestStore(t)
	exitCode, _ := runCopyJob(t, src, dst, store)
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	it, err := store.Iter()
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer it.Close()

	doneCount := 0
	totalCount := 0
	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		if len(key) >= 2 && key[0] == '_' && key[1] == '_' {
			continue
		}
		totalCount++
		cs, _ := it.Value()
		if cs == model.CopyDone {
			doneCount++
		}
	}

	if totalCount != 5 {
		t.Fatalf("expected 5 DB entries, got %d", totalCount)
	}
	if doneCount != 5 {
		t.Fatalf("expected 5 done entries, got %d", doneCount)
	}

	has, _ := store.HasWalkComplete()
	if !has {
		t.Fatal("expected __walk_complete")
	}
}

// Test 5: Context cancellation — walk incomplete, __walk_complete absent
func TestIntegration_ContextCancel(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	CreateTestTree(src, 5000)

	store := openTestStore(t)
	srcObj, _ := local.NewSource(src)
	dstObj, _ := local.NewDestination(dst)

	job := copy.NewJob(srcObj, dstObj, store, copy.WithParallelism(1))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	job.Run(ctx)

	has, _ := store.HasWalkComplete()
	if has {
		t.Fatal("expected NO __walk_complete after cancellation")
	}
}

// Test 6: EnsureDirMtime — directory timestamps match source
func TestIntegration_EnsureDirMtime(t *testing.T) {
	if IsRoot() {
		t.Skip("skipping mtime test as root (mtime set behaves differently)")
	}

	src := t.TempDir()
	dst := t.TempDir()

	os.MkdirAll(filepath.Join(src, "sub1", "deep"), 0o755)
	os.WriteFile(filepath.Join(src, "sub1", "deep", "f.txt"), []byte("x"), 0o644)

	oldTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.Local)
	os.Chtimes(filepath.Join(src, "sub1"), oldTime, oldTime)
	os.Chtimes(filepath.Join(src, "sub1", "deep"), oldTime, oldTime)

	store := openTestStore(t)
	exitCode, err := runCopyJob(t, src, dst, store, copy.WithEnsureDirMtime(true), copy.WithDstBase(dst))
	if err != nil {
		t.Fatalf("copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	if err := DirsHaveSameMtime(src, dst); err != nil {
		t.Fatalf("mtime check: %v", err)
	}
}

// Test 8: Resume with walk_complete — resume copies remaining files
func TestIntegration_ResumeWithWalkComplete(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	os.MkdirAll(filepath.Join(src, "subdir"), 0o755)
	os.WriteFile(filepath.Join(src, "file1.txt"), []byte("hello"), 0o644)
	os.WriteFile(filepath.Join(src, "file2.txt"), []byte("world"), 0o644)
	os.WriteFile(filepath.Join(src, "subdir", "file3.txt"), []byte("test"), 0o644)

	store := openTestStore(t)
	srcObj, _ := local.NewSource(src)
	dstObj, _ := local.NewDestination(dst)

	exitCode, err := runCopyJob(t, src, dst, store)
	if err != nil {
		t.Fatalf("first copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	if err := store.Set("file2.txt", model.CopyDiscovered, model.CksumNone); err != nil {
		t.Fatalf("set discovered: %v", err)
	}
	os.Remove(filepath.Join(dst, "file2.txt"))

	job := copy.NewJob(srcObj, dstObj, store, copy.WithResume(true))
	exitCode, err = job.Run(context.Background())
	if err != nil {
		t.Fatalf("resume job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0 on resume, got %d", exitCode)
	}

	data, err := os.ReadFile(filepath.Join(dst, "file2.txt"))
	if err != nil || string(data) != "world" {
		t.Fatalf("file2.txt content mismatch after resume: %q, err %v", string(data), err)
	}
}

// Test 9: Resume without walk_complete — destroys DB and starts fresh
func TestIntegration_ResumeWithoutWalkComplete(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	os.WriteFile(filepath.Join(src, "file1.txt"), []byte("hello"), 0o644)
	os.WriteFile(filepath.Join(src, "file2.txt"), []byte("world"), 0o644)

	store := openTestStore(t)
	store.Set("file1.txt", model.CopyDiscovered, model.CksumNone)

	srcObj, _ := local.NewSource(src)
	dstObj, _ := local.NewDestination(dst)

	job := copy.NewJob(srcObj, dstObj, store, copy.WithResume(true))
	exitCode, err := job.Run(context.Background())
	if err != nil {
		t.Fatalf("resume job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	data, _ := os.ReadFile(filepath.Join(dst, "file1.txt"))
	if string(data) != "hello" {
		t.Fatalf("file1.txt mismatch after resume")
	}
	data, _ = os.ReadFile(filepath.Join(dst, "file2.txt"))
	if string(data) != "world" {
		t.Fatalf("file2.txt mismatch after resume")
	}

	has, _ := store.HasWalkComplete()
	if !has {
		t.Fatal("expected __walk_complete after fresh walk from resume")
	}
}

// Test 7: Error file handling — unreadable file produces error status
func TestIntegration_ErrorFileHandling(t *testing.T) {
	if IsRoot() {
		t.Skip("skipping permission test as root")
	}

	src := t.TempDir()
	dst := t.TempDir()

	os.WriteFile(filepath.Join(src, "good.txt"), []byte("ok"), 0o644)
	unreadablePath := filepath.Join(src, "bad.txt")
	os.WriteFile(unreadablePath, []byte("nope"), 0o644)
	MakeUnreadable(unreadablePath)
	defer MakeReadable(unreadablePath)

	store := openTestStore(t)
	srcObj, _ := local.NewSource(src)
	dstObj, _ := local.NewDestination(dst)

	job := copy.NewJob(srcObj, dstObj, store, copy.WithParallelism(1))
	exitCode, err := job.Run(context.Background())

	if exitCode != 2 {
		t.Fatalf("expected exit code 2, got %d", exitCode)
	}
	if err == nil {
		t.Fatal("expected error for partial failure")
	}

	data, err := os.ReadFile(filepath.Join(dst, "good.txt"))
	if err != nil || string(data) != "ok" {
		t.Fatalf("good.txt not copied correctly")
	}

	it, err := store.Iter()
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer it.Close()

	errorCount := 0
	doneCount := 0
	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		if len(key) >= 2 && key[0] == '_' && key[1] == '_' {
			continue
		}
		cs, _ := it.Value()
		if cs == model.CopyError {
			errorCount++
		} else if cs == model.CopyDone {
			doneCount++
		}
	}

	if errorCount == 0 {
		t.Fatal("expected at least 1 error entry in DB")
	}
	if doneCount == 0 {
		t.Fatal("expected at least 1 done entry in DB")
	}
}

// failAfterN wraps a Destination and makes OpenFile fail after N successful calls.
type failAfterN struct {
	storage.Destination
	mu     sync.Mutex
	count  int
	failAt int
}

func (d *failAfterN) OpenFile(relPath string, size int64, mode os.FileMode, uid, gid int) (storage.Writer, error) {
	d.mu.Lock()
	d.count++
	if d.count > d.failAt {
		d.mu.Unlock()
		return nil, fmt.Errorf("simulated disk error after %d files", d.failAt)
	}
	d.mu.Unlock()
	return d.Destination.OpenFile(relPath, size, mode, uid, gid)
}

// cancelAfterWalkSource wraps a Source and cancels context after N Walk callbacks.
type cancelAfterWalkSource struct {
	storage.Source
	mu     sync.Mutex
	count  int
	limit  int
	cancel context.CancelFunc
}

func (s *cancelAfterWalkSource) Walk(ctx context.Context, fn func(model.DiscoverItem) error) error {
	return s.Source.Walk(ctx, func(item model.DiscoverItem) error {
		s.mu.Lock()
		s.count++
		if s.count >= s.limit {
			s.cancel()
		}
		s.mu.Unlock()
		return fn(item)
	})
}

// Test 10: Resume after real cancellation
func TestIntegration_ResumeAfterCancellation(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	const fileCount = 3000
	if err := CreateTestTree(src, fileCount); err != nil {
		t.Fatalf("create test tree: %v", err)
	}

	store := openTestStore(t)
	srcObj, _ := local.NewSource(src)
	dstObj, _ := local.NewDestination(dst)

	ctx, cancel := context.WithCancel(context.Background())
	cancelSrc := &cancelAfterWalkSource{Source: srcObj, limit: 1000, cancel: cancel}

	job := copy.NewJob(cancelSrc, dstObj, store, copy.WithParallelism(1), copy.WithChannelBuf(1))
	job.Run(ctx)

	has, _ := store.HasWalkComplete()
	if has {
		t.Fatal("expected NO __walk_complete — walk was cancelled")
	}

	t.Logf("Walk cancelled after %d items discovered", cancelSrc.count)

	job2 := copy.NewJob(srcObj, dstObj, store, copy.WithResume(true))
	exitCode, err := job2.Run(context.Background())
	if err != nil {
		t.Fatalf("resume job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	if err := VerifyCopy(src, dst); err != nil {
		t.Fatalf("verify copy after resume: %v", err)
	}

	has, _ = store.HasWalkComplete()
	if !has {
		t.Fatal("expected __walk_complete after resume")
	}
}

// Test 11: Resume after partial failure
func TestIntegration_ResumeAfterPartialFailure(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	const fileCount = 500
	if err := CreateTestTree(src, fileCount); err != nil {
		t.Fatalf("create test tree: %v", err)
	}

	store := openTestStore(t)
	srcObj, _ := local.NewSource(src)
	realDst, _ := local.NewDestination(dst)

	const failAt = 200
	failDst := &failAfterN{Destination: realDst, failAt: failAt}

	job := copy.NewJob(srcObj, failDst, store, copy.WithParallelism(4))
	exitCode, err := job.Run(context.Background())

	if exitCode != 2 {
		t.Fatalf("expected exit code 2, got %d", exitCode)
	}
	if err == nil {
		t.Fatal("expected error for partial failure")
	}

	has, _ := store.HasWalkComplete()
	if !has {
		t.Fatal("expected __walk_complete after partial failure")
	}

	dstRegulars, _, _, _ := CountFiles(dst)
	srcRegulars, _, _, _ := CountFiles(src)
	t.Logf("After partial failure: %d/%d regular files in dst", dstRegulars, srcRegulars)
	if dstRegulars == 0 {
		t.Fatal("expected some files to be copied before failure")
	}
	if dstRegulars >= srcRegulars {
		t.Fatal("expected some regular files to be missing after partial failure")
	}

	it, err := store.Iter()
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	doneCount := 0
	errorCount := 0
	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		if len(key) >= 2 && key[0] == '_' && key[1] == '_' {
			continue
		}
		cs, _ := it.Value()
		if cs == model.CopyDone {
			doneCount++
		} else if cs == model.CopyError {
			errorCount++
		}
	}
	it.Close()

	if doneCount == 0 {
		t.Fatal("expected some CopyDone entries in DB")
	}
	if errorCount == 0 {
		t.Fatal("expected some CopyError entries in DB")
	}
	t.Logf("DB state: %d done, %d error", doneCount, errorCount)

	job2 := copy.NewJob(srcObj, realDst, store, copy.WithResume(true))
	exitCode, err = job2.Run(context.Background())
	if err != nil {
		t.Fatalf("resume job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	if err := VerifyCopy(src, dst); err != nil {
		t.Fatalf("verify copy after resume: %v", err)
	}
}
