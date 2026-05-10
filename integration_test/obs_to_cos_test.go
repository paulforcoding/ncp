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

// --- OBS → COS copy ---

func TestIntegration_OBSToCOS_Copy(t *testing.T) {
	envOBS := requireOBS(t)
	envCOS := requireCOS(t)

	srcPrefix := newOBSPrefix(t, envOBS, "obs2cos-copy-src")
	dstPrefix := newCOSPrefix(t, envCOS, "obs2cos-copy-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedOBSPrefix(t, envOBS, srcPrefix, files)

	src := newOBSSource(t, envOBS, srcPrefix)
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

func TestIntegration_OBSToCOS_Copy_Resume(t *testing.T) {
	envOBS := requireOBS(t)
	envCOS := requireCOS(t)

	srcPrefix := newOBSPrefix(t, envOBS, "obs2cos-copy-r-src")
	dstPrefix := newCOSPrefix(t, envCOS, "obs2cos-copy-r-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
		"c.txt":        "gamma",
	}
	seedOBSPrefix(t, envOBS, srcPrefix, files)

	src := newOBSSource(t, envOBS, srcPrefix)
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

// --- OBS → COS cksum ---

func TestIntegration_OBSToCOS_Cksum(t *testing.T) {
	envOBS := requireOBS(t)
	envCOS := requireCOS(t)

	srcPrefix := newOBSPrefix(t, envOBS, "obs2cos-cksum-src")
	dstPrefix := newCOSPrefix(t, envCOS, "obs2cos-cksum-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedOBSPrefix(t, envOBS, srcPrefix, files)
	seedCOSPrefix(t, envCOS, dstPrefix, files)

	src := newOBSSource(t, envOBS, srcPrefix)
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

func TestIntegration_OBSToCOS_Cksum_Resume(t *testing.T) {
	envOBS := requireOBS(t)
	envCOS := requireCOS(t)

	srcPrefix := newOBSPrefix(t, envOBS, "obs2cos-cksum-r-src")
	dstPrefix := newCOSPrefix(t, envCOS, "obs2cos-cksum-r-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedOBSPrefix(t, envOBS, srcPrefix, files)
	seedCOSPrefix(t, envCOS, dstPrefix, files)
	putCOSObject(t, envCOS, dstPrefix, "a.txt", "DIFFERENT")

	src := newOBSSource(t, envOBS, srcPrefix)
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
