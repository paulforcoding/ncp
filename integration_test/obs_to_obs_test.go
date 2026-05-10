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

// --- OBS → OBS copy ---

func TestIntegration_OBSToOBS_Copy(t *testing.T) {
	env := requireOBS(t)
	srcPrefix := newOBSPrefix(t, env, "obs2obs-copy-src")
	dstPrefix := newOBSPrefix(t, env, "obs2obs-copy-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedOBSPrefix(t, env, srcPrefix, files)

	src := newOBSSource(t, env, srcPrefix)
	dst := newOBSDestination(t, env, dstPrefix)
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

	verifyOBSPrefix(t, env, dstPrefix, files)
}

func TestIntegration_OBSToOBS_Copy_Resume(t *testing.T) {
	env := requireOBS(t)
	srcPrefix := newOBSPrefix(t, env, "obs2obs-copy-r-src")
	dstPrefix := newOBSPrefix(t, env, "obs2obs-copy-r-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
		"c.txt":        "gamma",
	}
	seedOBSPrefix(t, env, srcPrefix, files)

	src := newOBSSource(t, env, srcPrefix)
	realDst := newOBSDestination(t, env, dstPrefix)
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

	verifyOBSPrefix(t, env, dstPrefix, files)
}

// --- OBS → OBS cksum ---

func TestIntegration_OBSToOBS_Cksum(t *testing.T) {
	env := requireOBS(t)
	srcPrefix := newOBSPrefix(t, env, "obs2obs-cksum-src")
	dstPrefix := newOBSPrefix(t, env, "obs2obs-cksum-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedOBSPrefix(t, env, srcPrefix, files)
	seedOBSPrefix(t, env, dstPrefix, files)

	src := newOBSSource(t, env, srcPrefix)
	dst := newOBSSource(t, env, dstPrefix)
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
	putOBSObject(t, env, dstPrefix, "a.txt", "DIFFERENT")
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

func TestIntegration_OBSToOBS_Cksum_Resume(t *testing.T) {
	env := requireOBS(t)
	srcPrefix := newOBSPrefix(t, env, "obs2obs-cksum-r-src")
	dstPrefix := newOBSPrefix(t, env, "obs2obs-cksum-r-dst")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedOBSPrefix(t, env, srcPrefix, files)
	seedOBSPrefix(t, env, dstPrefix, files)
	putOBSObject(t, env, dstPrefix, "a.txt", "DIFFERENT")

	src := newOBSSource(t, env, srcPrefix)
	dst := newOBSSource(t, env, dstPrefix)
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

	putOBSObject(t, env, dstPrefix, "a.txt", "alpha")

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
