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
	"github.com/zp001/ncp/pkg/impls/storage/remote"
	"github.com/zp001/ncp/pkg/model"
)

// --- Remote → OSS copy ---

// TestIntegration_RemoteToOSS_Copy covers matrix case #9 (Remote→OSS, copy, no-resume).
func TestIntegration_RemoteToOSS_Copy(t *testing.T) {
	env := requireOSS(t)
	serveDir := t.TempDir()
	dstPrefix := newOSSPrefix(t, env, "remote2oss-copy-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	for relPath, content := range files {
		path := filepath.Join(serveDir, relPath)
		os.MkdirAll(filepath.Dir(path), 0o755)
		os.WriteFile(path, []byte(content), 0o644)
	}

	addr := startTestServer(t, serveDir)

	src, err := remote.NewSource(addr, serveDir)
	if err != nil {
		t.Fatalf("new remote source: %v", err)
	}
	dst := newOSSDestination(t, env, dstPrefix)
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

	verifyOSSPrefix(t, env, dstPrefix, files)
}

// TestIntegration_RemoteToOSS_Copy_Resume covers matrix case #10 (Remote→OSS, copy, resume).
func TestIntegration_RemoteToOSS_Copy_Resume(t *testing.T) {
	env := requireOSS(t)
	serveDir := t.TempDir()
	dstPrefix := newOSSPrefix(t, env, "remote2oss-copy-r-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
		"c.txt":        "gamma",
	}
	for relPath, content := range files {
		path := filepath.Join(serveDir, relPath)
		os.MkdirAll(filepath.Dir(path), 0o755)
		os.WriteFile(path, []byte(content), 0o644)
	}

	addr := startTestServer(t, serveDir)

	src, err := remote.NewSource(addr, serveDir)
	if err != nil {
		t.Fatalf("new remote source: %v", err)
	}
	realDst := newOSSDestination(t, env, dstPrefix)
	store := openTestStore(t)

	failDst := &failAfterN{Destination: realDst, failAt: 1}

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

	verifyOSSPrefix(t, env, dstPrefix, files)
}

// --- Remote → OSS cksum ---

// TestIntegration_RemoteToOSS_Cksum covers matrix case #11 (Remote→OSS, cksum, no-resume).
func TestIntegration_RemoteToOSS_Cksum(t *testing.T) {
	env := requireOSS(t)
	serveDir := t.TempDir()
	dstPrefix := newOSSPrefix(t, env, "remote2oss-cksum-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	for relPath, content := range files {
		path := filepath.Join(serveDir, relPath)
		os.MkdirAll(filepath.Dir(path), 0o755)
		os.WriteFile(path, []byte(content), 0o644)
	}
	seedOSSPrefix(t, env, dstPrefix, files)

	addr := startTestServer(t, serveDir)

	src, err := remote.NewSource(addr, serveDir)
	if err != nil {
		t.Fatalf("new remote source: %v", err)
	}
	dst := newOSSSource(t, env, dstPrefix)
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
	putOSSObject(t, env, dstPrefix, "a.txt", "DIFFERENT")
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

// TestIntegration_RemoteToOSS_Cksum_Resume covers matrix case #12 (Remote→OSS, cksum, resume).
func TestIntegration_RemoteToOSS_Cksum_Resume(t *testing.T) {
	env := requireOSS(t)
	serveDir := t.TempDir()
	dstPrefix := newOSSPrefix(t, env, "remote2oss-cksum-r-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	for relPath, content := range files {
		path := filepath.Join(serveDir, relPath)
		os.MkdirAll(filepath.Dir(path), 0o755)
		os.WriteFile(path, []byte(content), 0o644)
	}
	seedOSSPrefix(t, env, dstPrefix, files)
	// introduce mismatch
	putOSSObject(t, env, dstPrefix, "a.txt", "DIFFERENT")

	addr := startTestServer(t, serveDir)

	src, err := remote.NewSource(addr, serveDir)
	if err != nil {
		t.Fatalf("new remote source: %v", err)
	}
	dst := newOSSSource(t, env, dstPrefix)
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
	putOSSObject(t, env, dstPrefix, "a.txt", "alpha")

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
