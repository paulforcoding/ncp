//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zp001/ncp/internal/cksum"
	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/pkg/impls/storage/local"
	"github.com/zp001/ncp/pkg/model"
)

func TestIntegration_OBSToLocal_Copy(t *testing.T) {
	env := requireOBS(t)
	srcPrefix := newOBSPrefix(t, env, "obs2local-copy-src")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedOBSPrefix(t, env, srcPrefix, files)

	src := newOBSSource(t, env, srcPrefix)
	dstDir := t.TempDir()
	dst, _ := local.NewDestination(dstDir)
	store := openTestStore(t)

	job := copy.NewJob(src, dst, store,
		copy.WithParallelism(2),
		copy.WithCksumAlgo(model.CksumMD5),
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

func TestIntegration_OBSToLocal_Copy_Resume(t *testing.T) {
	env := requireOBS(t)
	srcPrefix := newOBSPrefix(t, env, "obs2local-copy-r-src")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
		"c.txt":        "gamma",
	}
	seedOBSPrefix(t, env, srcPrefix, files)

	src := newOBSSource(t, env, srcPrefix)
	dstDir := t.TempDir()
	realDst, _ := local.NewDestination(dstDir)
	store := openTestStore(t)

	failDst := newFailAfterNShared(realDst, 1)

	job := copy.NewJob(src, failDst, store,
		copy.WithParallelism(2),
		copy.WithCksumAlgo(model.CksumMD5),
	)

	exitCode, err := job.Run(context.Background())
	if exitCode != 2 {
		t.Fatalf("expected exit code 2, got %d", exitCode)
	}
	if err == nil {
		t.Fatal("expected error for partial failure")
	}

	job2 := copy.NewJob(src, realDst, store,
		copy.WithResume(true),
		copy.WithCksumAlgo(model.CksumMD5),
	)

	exitCode, err = job2.Run(context.Background())
	if err != nil {
		t.Fatalf("resume job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0 on resume, got %d", exitCode)
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

// --- OBS → Local cksum ---

func TestIntegration_OBSToLocal_Cksum(t *testing.T) {
	env := requireOBS(t)
	srcPrefix := newOBSPrefix(t, env, "obs2local-cksum-src")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedOBSPrefix(t, env, srcPrefix, files)

	dstDir := t.TempDir()
	for relPath, content := range files {
		path := filepath.Join(dstDir, relPath)
		os.MkdirAll(filepath.Dir(path), 0o755)
		os.WriteFile(path, []byte(content), 0o644)
	}

	src := newOBSSource(t, env, srcPrefix)
	dst, err := local.NewSource(dstDir)
	if err != nil {
		t.Fatalf("new local source: %v", err)
	}
	store := openTestStore(t)

	job := cksum.NewCksumJob(src, dst, store,
		cksum.WithCksumParallelism(2),
		cksum.WithCksumAlgo(model.CksumMD5),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("cksum job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	// mismatch branch
	os.WriteFile(filepath.Join(dstDir, "a.txt"), []byte("DIFFERENT"), 0o644)
	store2 := openTestStore(t)
	job2 := cksum.NewCksumJob(src, dst, store2,
		cksum.WithCksumParallelism(2),
		cksum.WithCksumAlgo(model.CksumMD5),
	)
	exitCode, err = job2.Run(ctx)
	if err == nil {
		t.Fatal("expected error for mismatch")
	}
	if exitCode != 2 {
		t.Fatalf("expected exit code 2, got %d", exitCode)
	}
}

func TestIntegration_OBSToLocal_Cksum_Resume(t *testing.T) {
	env := requireOBS(t)
	srcPrefix := newOBSPrefix(t, env, "obs2local-cksum-r-src")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedOBSPrefix(t, env, srcPrefix, files)

	dstDir := t.TempDir()
	for relPath, content := range files {
		path := filepath.Join(dstDir, relPath)
		os.MkdirAll(filepath.Dir(path), 0o755)
		os.WriteFile(path, []byte(content), 0o644)
	}
	// introduce mismatch
	os.WriteFile(filepath.Join(dstDir, "a.txt"), []byte("DIFFERENT"), 0o644)

	src := newOBSSource(t, env, srcPrefix)
	dst, err := local.NewSource(dstDir)
	if err != nil {
		t.Fatalf("new local source: %v", err)
	}
	store := openTestStore(t)

	job := cksum.NewCksumJob(src, dst, store,
		cksum.WithCksumParallelism(2),
		cksum.WithCksumAlgo(model.CksumMD5),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if exitCode != 2 {
		t.Fatalf("expected exit code 2, got %d", exitCode)
	}
	if err == nil {
		t.Fatal("expected error for mismatch")
	}

	// fix the mismatch
	os.WriteFile(filepath.Join(dstDir, "a.txt"), []byte("alpha"), 0o644)

	job2 := cksum.NewCksumJob(src, dst, store,
		cksum.WithCksumResume(true),
		cksum.WithCksumParallelism(2),
		cksum.WithCksumAlgo(model.CksumMD5),
	)

	exitCode, err = job2.Run(ctx)
	if err != nil {
		t.Fatalf("resume cksum job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0 on resume, got %d", exitCode)
	}
}
