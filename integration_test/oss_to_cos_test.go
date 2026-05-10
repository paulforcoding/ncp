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

// --- OSS → COS copy ---

// TestIntegration_OSSToCOS_Copy covers OSS→COS cross-cloud copy (no-resume).
func TestIntegration_OSSToCOS_Copy(t *testing.T) {
	// Both OSS and COS env are required
	envOSS := requireOSS(t)
	envCOS := requireCOS(t)

	srcPrefix := newOSSPrefix(t, envOSS, "oss2cos-copy-src")
	dstPrefix := newCOSPrefix(t, envCOS, "oss2cos-copy-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedOSSPrefix(t, envOSS, srcPrefix, files)

	src := newOSSSource(t, envOSS, srcPrefix)
	dst := newCOSDestination(t, envCOS, dstPrefix)
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

	verifyCOSPrefix(t, envCOS, dstPrefix, files)
}

// TestIntegration_OSSToCOS_Copy_Resume covers OSS→COS cross-cloud copy with resume.
func TestIntegration_OSSToCOS_Copy_Resume(t *testing.T) {
	envOSS := requireOSS(t)
	envCOS := requireCOS(t)

	srcPrefix := newOSSPrefix(t, envOSS, "oss2cos-copy-r-src")
	dstPrefix := newCOSPrefix(t, envCOS, "oss2cos-copy-r-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
		"c.txt":        "gamma",
	}
	seedOSSPrefix(t, envOSS, srcPrefix, files)

	src := newOSSSource(t, envOSS, srcPrefix)
	realDst := newCOSDestination(t, envCOS, dstPrefix)
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

	verifyCOSPrefix(t, envCOS, dstPrefix, files)
}

// --- OSS → COS cksum ---

// TestIntegration_OSSToCOS_Cksum covers OSS→COS cross-cloud cksum (no-resume).
func TestIntegration_OSSToCOS_Cksum(t *testing.T) {
	envOSS := requireOSS(t)
	envCOS := requireCOS(t)

	srcPrefix := newOSSPrefix(t, envOSS, "oss2cos-cksum-src")
	dstPrefix := newCOSPrefix(t, envCOS, "oss2cos-cksum-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedOSSPrefix(t, envOSS, srcPrefix, files)
	seedCOSPrefix(t, envCOS, dstPrefix, files)

	src := newOSSSource(t, envOSS, srcPrefix)
	dst := newCOSSource(t, envCOS, dstPrefix)
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
	putCOSObject(t, envCOS, dstPrefix, "a.txt", "DIFFERENT")
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

// TestIntegration_OSSToCOS_Cksum_Resume covers OSS→COS cross-cloud cksum with resume.
func TestIntegration_OSSToCOS_Cksum_Resume(t *testing.T) {
	envOSS := requireOSS(t)
	envCOS := requireCOS(t)

	srcPrefix := newOSSPrefix(t, envOSS, "oss2cos-cksum-r-src")
	dstPrefix := newCOSPrefix(t, envCOS, "oss2cos-cksum-r-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedOSSPrefix(t, envOSS, srcPrefix, files)
	seedCOSPrefix(t, envCOS, dstPrefix, files)
	// introduce mismatch
	putCOSObject(t, envCOS, dstPrefix, "a.txt", "DIFFERENT")

	src := newOSSSource(t, envOSS, srcPrefix)
	dst := newCOSSource(t, envCOS, dstPrefix)
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
	putCOSObject(t, envCOS, dstPrefix, "a.txt", "alpha")

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
