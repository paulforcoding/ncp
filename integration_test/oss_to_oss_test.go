//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/zp001/ncp/internal/cksum"
	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/pkg/model"
)

// --- OSS → OSS copy ---

// TestIntegration_OSSToOSS_Copy covers matrix case #1 (OSS→OSS, copy, no-resume).
func TestIntegration_OSSToOSS_Copy(t *testing.T) {
	env := requireOSS(t)
	srcPrefix := newOSSPrefix(t, env, "oss2oss-copy-src")
	dstPrefix := newOSSPrefix(t, env, "oss2oss-copy-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedOSSPrefix(t, env, srcPrefix, files)

	src := newOSSSource(t, env, srcPrefix)
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

// TestIntegration_OSSToOSS_Copy_Resume covers matrix case #2 (OSS→OSS, copy, resume).
func TestIntegration_OSSToOSS_Copy_Resume(t *testing.T) {
	env := requireOSS(t)
	srcPrefix := newOSSPrefix(t, env, "oss2oss-copy-r-src")
	dstPrefix := newOSSPrefix(t, env, "oss2oss-copy-r-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
		"c.txt":        "gamma",
	}
	seedOSSPrefix(t, env, srcPrefix, files)

	src := newOSSSource(t, env, srcPrefix)
	realDst := newOSSDestination(t, env, dstPrefix)
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

	verifyOSSPrefix(t, env, dstPrefix, files)
}

// --- OSS → OSS cksum ---

// TestIntegration_OSSToOSS_Cksum covers matrix case #3 (OSS→OSS, cksum, no-resume).
func TestIntegration_OSSToOSS_Cksum(t *testing.T) {
	env := requireOSS(t)
	srcPrefix := newOSSPrefix(t, env, "oss2oss-cksum-src")
	dstPrefix := newOSSPrefix(t, env, "oss2oss-cksum-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedOSSPrefix(t, env, srcPrefix, files)
	seedOSSPrefix(t, env, dstPrefix, files)

	src := newOSSSource(t, env, srcPrefix)
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

// TestIntegration_OSSToOSS_Cksum_Resume covers matrix case #4 (OSS→OSS, cksum, resume).
func TestIntegration_OSSToOSS_Cksum_Resume(t *testing.T) {
	env := requireOSS(t)
	srcPrefix := newOSSPrefix(t, env, "oss2oss-cksum-r-src")
	dstPrefix := newOSSPrefix(t, env, "oss2oss-cksum-r-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedOSSPrefix(t, env, srcPrefix, files)
	seedOSSPrefix(t, env, dstPrefix, files)
	// introduce mismatch
	putOSSObject(t, env, dstPrefix, "a.txt", "DIFFERENT")

	src := newOSSSource(t, env, srcPrefix)
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
