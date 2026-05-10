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

func TestIntegration_COSToLocal_Copy(t *testing.T) {
	env := requireCOS(t)
	srcPrefix := newCOSPrefix(t, env, "cos2local-copy-src")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedCOSPrefix(t, env, srcPrefix, files)

	src := newCOSSource(t, env, srcPrefix)
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

func TestIntegration_COSToLocal_Copy_Resume(t *testing.T) {
	env := requireCOS(t)
	srcPrefix := newCOSPrefix(t, env, "cos2local-copy-r-src")

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
		"c.txt":        "gamma",
	}
	seedCOSPrefix(t, env, srcPrefix, files)

	src := newCOSSource(t, env, srcPrefix)
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
