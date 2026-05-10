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

// --- COS → OBS copy ---

func TestIntegration_COSToOBS_Copy(t *testing.T) {
	envCOS := requireCOS(t)
	envOBS := requireOBS(t)

	srcPrefix := newCOSPrefix(t, envCOS, "cos2obs-copy-src")
	dstPrefix := newOBSPrefix(t, envOBS, "cos2obs-copy-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedCOSPrefix(t, envCOS, srcPrefix, files)

	src := newCOSSource(t, envCOS, srcPrefix)
	dst := newOBSDestination(t, envOBS, dstPrefix)
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

	verifyOBSPrefix(t, envOBS, dstPrefix, files)
}

func TestIntegration_COSToOBS_Copy_Resume(t *testing.T) {
	envCOS := requireCOS(t)
	envOBS := requireOBS(t)

	srcPrefix := newCOSPrefix(t, envCOS, "cos2obs-copy-r-src")
	dstPrefix := newOBSPrefix(t, envOBS, "cos2obs-copy-r-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
		"c.txt":        "gamma",
	}
	seedCOSPrefix(t, envCOS, srcPrefix, files)

	src := newCOSSource(t, envCOS, srcPrefix)
	realDst := newOBSDestination(t, envOBS, dstPrefix)
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

	verifyOBSPrefix(t, envOBS, dstPrefix, files)
}

// --- COS → OBS cksum ---

func TestIntegration_COSToOBS_Cksum(t *testing.T) {
	envCOS := requireCOS(t)
	envOBS := requireOBS(t)

	srcPrefix := newCOSPrefix(t, envCOS, "cos2obs-cksum-src")
	dstPrefix := newOBSPrefix(t, envOBS, "cos2obs-cksum-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedCOSPrefix(t, envCOS, srcPrefix, files)
	seedOBSPrefix(t, envOBS, dstPrefix, files)

	src := newCOSSource(t, envCOS, srcPrefix)
	dst := newOBSSource(t, envOBS, dstPrefix)
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

	putOBSObject(t, envOBS, dstPrefix, "a.txt", "DIFFERENT")
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

func TestIntegration_COSToOBS_Cksum_Resume(t *testing.T) {
	envCOS := requireCOS(t)
	envOBS := requireOBS(t)

	srcPrefix := newCOSPrefix(t, envCOS, "cos2obs-cksum-r-src")
	dstPrefix := newOBSPrefix(t, envOBS, "cos2obs-cksum-r-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedCOSPrefix(t, envCOS, srcPrefix, files)
	seedOBSPrefix(t, envOBS, dstPrefix, files)
	putOBSObject(t, envOBS, dstPrefix, "a.txt", "DIFFERENT")

	src := newCOSSource(t, envCOS, srcPrefix)
	dst := newOBSSource(t, envOBS, dstPrefix)
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

	putOBSObject(t, envOBS, dstPrefix, "a.txt", "alpha")

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
