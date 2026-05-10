//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/pkg/impls/storage/local"
	"github.com/zp001/ncp/pkg/model"
)

func TestIntegration_LocalToOBS_Copy(t *testing.T) {
	env := requireOBS(t)
	dstPrefix := newOBSPrefix(t, env, "local2obs-copy-dst")

	srcDir := t.TempDir()
	os.MkdirAll(filepath.Join(srcDir, "subdir"), 0o755)
	os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("alpha"), 0o644)
	os.WriteFile(filepath.Join(srcDir, "subdir", "b.txt"), []byte("beta"), 0o644)

	src, err := local.NewSource(srcDir)
	if err != nil {
		t.Fatalf("new local source: %v", err)
	}
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

	verifyOBSPrefix(t, env, dstPrefix, map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	})
}

func TestIntegration_LocalToOBS_Copy_Resume(t *testing.T) {
	env := requireOBS(t)
	dstPrefix := newOBSPrefix(t, env, "local2obs-copy-r-dst")

	srcDir := t.TempDir()
	os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("alpha"), 0o644)
	os.WriteFile(filepath.Join(srcDir, "b.txt"), []byte("beta"), 0o644)
	os.WriteFile(filepath.Join(srcDir, "c.txt"), []byte("gamma"), 0o644)

	src, _ := local.NewSource(srcDir)
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

	verifyOBSPrefix(t, env, dstPrefix, map[string]string{
		"a.txt": "alpha",
		"b.txt": "beta",
		"c.txt": "gamma",
	})
}

func TestIntegration_LocalToOBS_Copy_LargeFile(t *testing.T) {
	env := requireOBS(t)
	dstPrefix := newOBSPrefix(t, env, "local2obs-large-dst")

	srcDir := t.TempDir()
	// 6MB file to trigger multipart (smallFileThreshold = 5MB).
	largeContent := make([]byte, 6<<20)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}
	os.WriteFile(filepath.Join(srcDir, "large.bin"), largeContent, 0o644)

	src, _ := local.NewSource(srcDir)
	dst := newOBSDestination(t, env, dstPrefix)
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

	client := newOBSClientHelper(t, env)
	md, err := client.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: env.Bucket,
		Key:    dstPrefix + "large.bin",
	})
	if err != nil {
		t.Fatalf("head large.bin: %v", err)
	}
	if md.ContentLength != int64(len(largeContent)) {
		t.Errorf("expected ContentLength=%d, got %d", len(largeContent), md.ContentLength)
	}
}
