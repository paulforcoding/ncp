//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/pkg/impls/storage/local"
	"github.com/zp001/ncp/pkg/model"
)

func TestIntegration_LocalToCOS_Copy(t *testing.T) {
	env := requireCOS(t)
	dstPrefix := newCOSPrefix(t, env, "local2cos-copy-dst")

	srcDir := t.TempDir()
	os.MkdirAll(filepath.Join(srcDir, "subdir"), 0o755)
	os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("alpha"), 0o644)
	os.WriteFile(filepath.Join(srcDir, "subdir", "b.txt"), []byte("beta"), 0o644)

	src, err := local.NewSource(srcDir)
	if err != nil {
		t.Fatalf("new local source: %v", err)
	}
	dst := newCOSDestination(t, env, dstPrefix)
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

	verifyCOSPrefix(t, env, dstPrefix, map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	})
}

func TestIntegration_LocalToCOS_Copy_Resume(t *testing.T) {
	env := requireCOS(t)
	dstPrefix := newCOSPrefix(t, env, "local2cos-copy-r-dst")

	srcDir := t.TempDir()
	os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("alpha"), 0o644)
	os.WriteFile(filepath.Join(srcDir, "b.txt"), []byte("beta"), 0o644)
	os.WriteFile(filepath.Join(srcDir, "c.txt"), []byte("gamma"), 0o644)

	src, _ := local.NewSource(srcDir)
	realDst := newCOSDestination(t, env, dstPrefix)
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

	verifyCOSPrefix(t, env, dstPrefix, map[string]string{
		"a.txt": "alpha",
		"b.txt": "beta",
		"c.txt": "gamma",
	})
}

func TestIntegration_LocalToCOS_Copy_LargeFile(t *testing.T) {
	env := requireCOS(t)
	dstPrefix := newCOSPrefix(t, env, "local2cos-large-dst")

	srcDir := t.TempDir()
	// Create a 2MB file to trigger multipart upload (COS minPartSize = 1MB)
	largeContent := make([]byte, 2<<20)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}
	os.WriteFile(filepath.Join(srcDir, "large.bin"), largeContent, 0o644)

	src, _ := local.NewSource(srcDir)
	dst := newCOSDestination(t, env, dstPrefix)
	store := openTestStore(t)

	job := copy.NewJob(src, dst, store,
		copy.WithParallelism(2),
		copy.WithCksumAlgo(model.CksumMD5),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	// Verify via COS that the file exists with correct size
	client := newCOSClient(env)
	resp, err := client.Object.Head(ctx, dstPrefix+"large.bin", nil)
	if err != nil {
		t.Fatalf("head large.bin: %v", err)
	}
	if resp.Header.Get("Content-Length") != "2097152" {
		t.Errorf("expected Content-Length=2097152, got %s", resp.Header.Get("Content-Length"))
	}
}
