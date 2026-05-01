//go:build integration

package copy

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zp001/ncp/internal/progress/pebble"
	"github.com/zp001/ncp/internal/storage/local"
	"github.com/zp001/ncp/pkg/model"
	"github.com/zp001/ncp/testutil"
)

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

func runCopyJob(t *testing.T, src, dst string, store *pebble.Store, opts ...JobOption) (int, error) {
	t.Helper()
	srcObj, err := local.NewSource(src)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	dstObj, err := local.NewDestination(dst)
	if err != nil {
		t.Fatalf("new destination: %v", err)
	}
	job := NewJob(srcObj, dstObj, store, opts...)
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

	if err := testutil.CreateTestTree(src, 1000); err != nil {
		t.Fatalf("create test tree: %v", err)
	}

	srcRegulars, srcDirs, _, _ := testutil.CountFiles(src)
	t.Logf("Source: %d regulars, %d dirs", srcRegulars, srcDirs)

	store := openTestStore(t)
	exitCode, err := runCopyJob(t, src, dst, store, WithParallelism(4))
	if err != nil {
		t.Fatalf("copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	if err := testutil.VerifyCopy(src, dst); err != nil {
		t.Fatalf("verify copy: %v", err)
	}
}

// Test 3: Channel full → Walker writes discovered, then dispatches remaining
func TestIntegration_ChannelFullDispatch(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	if err := testutil.CreateTestTree(src, 200); err != nil {
		t.Fatalf("create test tree: %v", err)
	}

	store := openTestStore(t)
	// Small buffer to force channel full
	exitCode, err := runCopyJob(t, src, dst, store,
		WithParallelism(1),
		WithBufferSizes(1, 1),
	)
	if err != nil {
		t.Fatalf("copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	if err := testutil.VerifyCopy(src, dst); err != nil {
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

	// All entries should be CopyDone
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

	// 3 files + 2 dirs = 5 entries
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

	// Create enough files that walk won't finish instantly
	testutil.CreateTestTree(src, 5000)

	store := openTestStore(t)
	srcObj, _ := local.NewSource(src)
	dstObj, _ := local.NewDestination(dst)

	job := NewJob(srcObj, dstObj, store, WithParallelism(1))

	// Cancel context after a short delay
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	exitCode, _ := job.Run(ctx)
	// May be 0 or 2 depending on timing, but __walk_complete must be absent
	_ = exitCode

	has, _ := store.HasWalkComplete()
	if has {
		t.Fatal("expected NO __walk_complete after cancellation")
	}
}

// Test 6: EnsureDirMtime — directory timestamps match source
func TestIntegration_EnsureDirMtime(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skipping mtime test as root (mtime set behaves differently)")
	}

	src := t.TempDir()
	dst := t.TempDir()

	// Create tree with specific mtime on directories
	os.MkdirAll(filepath.Join(src, "sub1", "deep"), 0o755)
	os.WriteFile(filepath.Join(src, "sub1", "deep", "f.txt"), []byte("x"), 0o644)

	// Set a known mtime on source directories
	oldTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.Local)
	os.Chtimes(filepath.Join(src, "sub1"), oldTime, oldTime)
	os.Chtimes(filepath.Join(src, "sub1", "deep"), oldTime, oldTime)

	store := openTestStore(t)
	exitCode, err := runCopyJob(t, src, dst, store, WithEnsureDirMtime(true), WithDstBase(dst))
	if err != nil {
		t.Fatalf("copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	// Check that destination dirs have the same mtime as source
	if err := testutil.DirsHaveSameMtime(src, dst); err != nil {
		t.Fatalf("mtime check: %v", err)
	}
}

// Test 8: Resume with walk_complete — resume copies remaining files
func TestIntegration_ResumeWithWalkComplete(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	// Create source tree
	os.MkdirAll(filepath.Join(src, "subdir"), 0o755)
	os.WriteFile(filepath.Join(src, "file1.txt"), []byte("hello"), 0o644)
	os.WriteFile(filepath.Join(src, "file2.txt"), []byte("world"), 0o644)
	os.WriteFile(filepath.Join(src, "subdir", "file3.txt"), []byte("test"), 0o644)

	store := openTestStore(t)
	srcObj, _ := local.NewSource(src)
	dstObj, _ := local.NewDestination(dst)

	// First run: copy file1 and file2, then cancel before all files done
	// Simulate by doing a full copy first, then manually marking some as discovered
	exitCode, err := runCopyJob(t, src, dst, store)
	if err != nil {
		t.Fatalf("first copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	// Now simulate a partially-completed state:
	// Mark file2.txt as CopyDiscovered (as if copy was interrupted before it was copied)
	if err := store.Set("file2.txt", model.CopyDiscovered, model.CksumNone); err != nil {
		t.Fatalf("set discovered: %v", err)
	}
	// Delete the destination file2.txt to simulate it wasn't copied
	os.Remove(filepath.Join(dst, "file2.txt"))

	// Resume: should only copy file2.txt
	job := NewJob(srcObj, dstObj, store, WithResume(true))
	exitCode, err = job.Run(context.Background())
	if err != nil {
		t.Fatalf("resume job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0 on resume, got %d", exitCode)
	}

	// Verify file2.txt was copied on resume
	data, err := os.ReadFile(filepath.Join(dst, "file2.txt"))
	if err != nil || string(data) != "world" {
		t.Fatalf("file2.txt content mismatch after resume: %q, err %v", string(data), err)
	}
}

// Test 9: Resume without walk_complete — destroys DB and starts fresh
func TestIntegration_ResumeWithoutWalkComplete(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	// Create source tree
	os.WriteFile(filepath.Join(src, "file1.txt"), []byte("hello"), 0o644)
	os.WriteFile(filepath.Join(src, "file2.txt"), []byte("world"), 0o644)

	store := openTestStore(t)

	// Simulate a walk that didn't complete:
	// Just write some entries directly without SetWalkComplete
	store.Set("file1.txt", model.CopyDiscovered, model.CksumNone)

	srcObj, _ := local.NewSource(src)
	dstObj, _ := local.NewDestination(dst)

	// Resume should detect no walk_complete and start fresh
	job := NewJob(srcObj, dstObj, store, WithResume(true))
	exitCode, err := job.Run(context.Background())
	if err != nil {
		t.Fatalf("resume job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	// Both files should be copied
	data, _ := os.ReadFile(filepath.Join(dst, "file1.txt"))
	if string(data) != "hello" {
		t.Fatalf("file1.txt mismatch after resume")
	}
	data, _ = os.ReadFile(filepath.Join(dst, "file2.txt"))
	if string(data) != "world" {
		t.Fatalf("file2.txt mismatch after resume")
	}

	// walk_complete should exist now
	has, _ := store.HasWalkComplete()
	if !has {
		t.Fatal("expected __walk_complete after fresh walk from resume")
	}
}

// Test 7: Error file handling — unreadable file produces error status
func TestIntegration_ErrorFileHandling(t *testing.T) {
	if testutil.IsRoot() {
		t.Skip("skipping permission test as root")
	}

	src := t.TempDir()
	dst := t.TempDir()

	os.WriteFile(filepath.Join(src, "good.txt"), []byte("ok"), 0o644)
	unreadablePath := filepath.Join(src, "bad.txt")
	os.WriteFile(unreadablePath, []byte("nope"), 0o644)
	testutil.MakeUnreadable(unreadablePath)
	defer testutil.MakeReadable(unreadablePath) // cleanup for TempDir removal

	store := openTestStore(t)
	srcObj, _ := local.NewSource(src)
	dstObj, _ := local.NewDestination(dst)

	job := NewJob(srcObj, dstObj, store, WithParallelism(1))
	exitCode, err := job.Run(context.Background())

	if exitCode != 2 {
		t.Fatalf("expected exit code 2, got %d", exitCode)
	}
	if err == nil {
		t.Fatal("expected error for partial failure")
	}

	// good.txt should be copied
	data, err := os.ReadFile(filepath.Join(dst, "good.txt"))
	if err != nil || string(data) != "ok" {
		t.Fatalf("good.txt not copied correctly")
	}

	// DB should have error entries for bad.txt
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
