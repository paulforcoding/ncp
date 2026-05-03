//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zp001/ncp/internal/cksum"
	"github.com/zp001/ncp/pkg/impls/storage/remote"
	"github.com/zp001/ncp/pkg/model"
)

// TestIntegration_RemoteCksum_NcpToNcp verifies cksum works with ncp:// on both sides.
// Covers matrix cells #15 (Remote→Remote, cksum, no-resume) for the pass branch and
// confirms mismatch detection too. The legacy "ncp:// destinations don't support cksum"
// constraint is removed; this test guards against regression.
func TestIntegration_RemoteCksum_NcpToNcp(t *testing.T) {
	t.Run("pass", func(t *testing.T) {
		serveDir := t.TempDir()
		srcDir := filepath.Join(serveDir, "src")
		dstDir := filepath.Join(serveDir, "dst")
		writeIdenticalTrees(t, srcDir, dstDir, map[string]string{
			"a.txt":        "alpha",
			"subdir/b.txt": "beta",
		})

		exitCode := runRemoteCksum(t, srcDir, dstDir)
		if exitCode != 0 {
			t.Fatalf("expected exit code 0 (pass), got %d", exitCode)
		}
	})

	t.Run("mismatch", func(t *testing.T) {
		serveDir := t.TempDir()
		srcDir := filepath.Join(serveDir, "src")
		dstDir := filepath.Join(serveDir, "dst")
		os.MkdirAll(srcDir, 0o755)
		os.MkdirAll(dstDir, 0o755)
		os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("alpha"), 0o644)
		os.WriteFile(filepath.Join(dstDir, "a.txt"), []byte("DIFFERENT"), 0o644)

		exitCode := runRemoteCksum(t, srcDir, dstDir)
		if exitCode != 2 {
			t.Fatalf("expected exit code 2 (mismatch), got %d", exitCode)
		}
	})
}

func writeIdenticalTrees(t *testing.T, srcDir, dstDir string, files map[string]string) {
	t.Helper()
	for _, root := range []string{srcDir, dstDir} {
		os.MkdirAll(root, 0o755)
		for rel, content := range files {
			path := filepath.Join(root, rel)
			os.MkdirAll(filepath.Dir(path), 0o755)
			os.WriteFile(path, []byte(content), 0o644)
		}
	}
}

func runRemoteCksum(t *testing.T, srcDir, dstDir string) int {
	t.Helper()
	addr := startTestServer(t, filepath.Dir(srcDir))

	src, err := remote.NewSource(addr, srcDir)
	if err != nil {
		t.Fatalf("new src: %v", err)
	}
	dst, err := remote.NewSource(addr, dstDir)
	if err != nil {
		t.Fatalf("new dst: %v", err)
	}

	store := openTestStore(t)
	job := cksum.NewCksumJob(src, dst, store,
		cksum.WithCksumParallelism(2),
		cksum.WithCksumAlgo(model.CksumMD5),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	exitCode, _ := job.Run(ctx)
	return exitCode
}

// TestIntegration_RemoteCksum_NcpToNcp_Resume covers matrix case #16
// (Remote→Remote, cksum, resume).
func TestIntegration_RemoteCksum_NcpToNcp_Resume(t *testing.T) {
	serveDir := t.TempDir()
	srcDir := filepath.Join(serveDir, "src")
	dstDir := filepath.Join(serveDir, "dst")
	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	writeIdenticalTrees(t, srcDir, dstDir, files)
	// introduce mismatch
	os.WriteFile(filepath.Join(dstDir, "a.txt"), []byte("DIFFERENT"), 0o644)

	addr := startTestServer(t, filepath.Dir(srcDir))

	src, _ := remote.NewSource(addr, srcDir)
	dst, _ := remote.NewSource(addr, dstDir)
	store := openTestStore(t)

	job := cksum.NewCksumJob(src, dst, store,
		cksum.WithCksumParallelism(2),
		cksum.WithCksumAlgo(model.CksumMD5),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if exitCode != 2 {
		t.Fatalf("expected exit code 2, got %d", exitCode)
	}
	if err == nil {
		t.Fatal("expected error for mismatch")
	}

	// fix the mismatch
	os.WriteFile(filepath.Join(dstDir, "a.txt"), []byte("alpha"), 0o644)

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
