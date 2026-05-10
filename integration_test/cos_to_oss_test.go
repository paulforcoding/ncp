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

// --- COS → OSS copy ---

// TestIntegration_COSToOSS_Copy covers COS→OSS cross-cloud copy (no-resume).
func TestIntegration_COSToOSS_Copy(t *testing.T) {
	// Both COS and OSS env are required
	envCOS := requireCOS(t)
	envOSS := requireOSS(t)

	srcPrefix := newCOSPrefix(t, envCOS, "cos2oss-copy-src")
	dstPrefix := newOSSPrefix(t, envOSS, "cos2oss-copy-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedCOSPrefix(t, envCOS, srcPrefix, files)

	src := newCOSSource(t, envCOS, srcPrefix)
	dst := newOSSDestination(t, envOSS, dstPrefix)
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

	verifyOSSPrefix(t, envOSS, dstPrefix, files)
}

// TestIntegration_COSToOSS_Copy_Resume covers COS→OSS cross-cloud copy with resume.
func TestIntegration_COSToOSS_Copy_Resume(t *testing.T) {
	envCOS := requireCOS(t)
	envOSS := requireOSS(t)

	srcPrefix := newCOSPrefix(t, envCOS, "cos2oss-copy-r-src")
	dstPrefix := newOSSPrefix(t, envOSS, "cos2oss-copy-r-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
		"c.txt":        "gamma",
	}
	seedCOSPrefix(t, envCOS, srcPrefix, files)

	src := newCOSSource(t, envCOS, srcPrefix)
	realDst := newOSSDestination(t, envOSS, dstPrefix)
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

	verifyOSSPrefix(t, envOSS, dstPrefix, files)
}

// --- COS → OSS cksum ---

// TestIntegration_COSToOSS_Cksum covers COS→OSS cross-cloud cksum (no-resume).
func TestIntegration_COSToOSS_Cksum(t *testing.T) {
	envCOS := requireCOS(t)
	envOSS := requireOSS(t)

	srcPrefix := newCOSPrefix(t, envCOS, "cos2oss-cksum-src")
	dstPrefix := newOSSPrefix(t, envOSS, "cos2oss-cksum-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedCOSPrefix(t, envCOS, srcPrefix, files)
	seedOSSPrefix(t, envOSS, dstPrefix, files)

	src := newCOSSource(t, envCOS, srcPrefix)
	dst := newOSSSource(t, envOSS, dstPrefix)
	store := openTestStore(t)

	job := cksum.NewCksumJob(src, dst, store,
		cksum.WithCksumParallelism(2),
		cksum.WithCksumAlgo(model.CksumMD5),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("cksum job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	// mismatch branch
	putOSSObject(t, envOSS, dstPrefix, "a.txt", "DIFFERENT")
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

// TestIntegration_COSToOSS_Cksum_Resume covers COS→OSS cross-cloud cksum with resume.
func TestIntegration_COSToOSS_Cksum_Resume(t *testing.T) {
	envCOS := requireCOS(t)
	envOSS := requireOSS(t)

	srcPrefix := newCOSPrefix(t, envCOS, "cos2oss-cksum-r-src")
	dstPrefix := newOSSPrefix(t, envOSS, "cos2oss-cksum-r-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedCOSPrefix(t, envCOS, srcPrefix, files)
	seedOSSPrefix(t, envOSS, dstPrefix, files)
	// introduce mismatch
	putOSSObject(t, envOSS, dstPrefix, "a.txt", "DIFFERENT")

	src := newCOSSource(t, envCOS, srcPrefix)
	dst := newOSSSource(t, envOSS, dstPrefix)
	store := openTestStore(t)

	job := cksum.NewCksumJob(src, dst, store,
		cksum.WithCksumParallelism(2),
		cksum.WithCksumAlgo(model.CksumMD5),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if exitCode != 2 {
		t.Fatalf("expected exit code 2, got %d", exitCode)
	}
	if err == nil {
		t.Fatal("expected error for mismatch")
	}

	// fix the mismatch
	putOSSObject(t, envOSS, dstPrefix, "a.txt", "alpha")

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
