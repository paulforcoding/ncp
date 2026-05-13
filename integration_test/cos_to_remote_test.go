//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/zp001/ncp/internal/cksum"
	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/pkg/impls/storage/remote"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// --- COS → Remote copy ---

func TestIntegration_COSToRemote_Copy(t *testing.T) {
	env := requireCOS(t)
	srcPrefix := newCOSPrefix(t, env, "cos2remote-copy-src")
	serveDir := t.TempDir()

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedCOSPrefix(t, env, srcPrefix, files)

	addr := startTestServer(t, serveDir)

	src := newCOSSource(t, env, srcPrefix)
	store := openTestStore(t)

	dstFactory := func(id int) (storage.Destination, error) {
		return remote.NewDestination(addr, serveDir)
	}

	job := copy.NewJob(src, nil, store,
		copy.WithParallelism(2),
		copy.WithDstFactory(dstFactory),
		copy.WithDstBase("ncp://"+addr),
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
		path := filepath.Join(serveDir, relPath)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", relPath, err)
		}
		if string(data) != want {
			t.Errorf("content mismatch %s: got %q, want %q", relPath, string(data), want)
		}
	}
}

func TestIntegration_COSToRemote_Copy_Resume(t *testing.T) {
	env := requireCOS(t)
	srcPrefix := newCOSPrefix(t, env, "cos2remote-copy-r-src")
	serveDir := t.TempDir()

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
		"c.txt":        "gamma",
	}
	seedCOSPrefix(t, env, srcPrefix, files)

	addr := startTestServer(t, serveDir)

	src := newCOSSource(t, env, srcPrefix)
	store := openTestStore(t)

	mu := &sync.Mutex{}
	count := 0
	dstFactory := func(id int) (storage.Destination, error) {
		dst, err := remote.NewDestination(addr, serveDir)
		if err != nil {
			return nil, err
		}
		return &failAfterNShared{Destination: dst, mu: mu, count: &count, failAt: 1}, nil
	}

	job := copy.NewJob(src, nil, store,
		copy.WithParallelism(2),
		copy.WithDstFactory(dstFactory),
		copy.WithDstBase("ncp://"+addr),
		copy.WithCksumAlgo(model.CksumMD5),
	)

	exitCode, err := job.Run(context.Background())
	if exitCode != 2 {
		t.Fatalf("expected exit code 2, got %d", exitCode)
	}
	if err == nil {
		t.Fatal("expected error for partial failure")
	}

	dstFactory2 := func(id int) (storage.Destination, error) {
		return remote.NewDestination(addr, serveDir)
	}

	job2 := copy.NewJob(src, nil, store,
		copy.WithResume(true),
		copy.WithTaskID(job.TaskID()),
		copy.WithDstFactory(dstFactory2),
		copy.WithDstBase("ncp://"+addr),
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
		path := filepath.Join(serveDir, relPath)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", relPath, err)
		}
		if string(data) != want {
			t.Errorf("content mismatch %s: got %q, want %q", relPath, string(data), want)
		}
	}
}

// --- COS → Remote cksum ---

func TestIntegration_COSToRemote_Cksum(t *testing.T) {
	env := requireCOS(t)
	srcPrefix := newCOSPrefix(t, env, "cos2remote-cksum-src")
	serveDir := t.TempDir()

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedCOSPrefix(t, env, srcPrefix, files)
	for relPath, content := range files {
		path := filepath.Join(serveDir, relPath)
		os.MkdirAll(filepath.Dir(path), 0o755)
		os.WriteFile(path, []byte(content), 0o644)
	}

	addr := startTestServer(t, serveDir)

	src := newCOSSource(t, env, srcPrefix)
	dst, err := remote.NewSource(addr, serveDir)
	if err != nil {
		t.Fatalf("new remote source: %v", err)
	}
	store := openTestStore(t)

	dstFactory := func(id int) (storage.Source, error) {
		return remote.NewSource(addr, serveDir)
	}

	job := cksum.NewCksumJob(src, dst, store,
		cksum.WithCksumParallelism(2),
		cksum.WithCksumDstFactory(dstFactory),
		cksum.WithCksumAlgo(model.CksumMD5),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := dst.BeginTask(ctx, job.TaskID()); err != nil {
		t.Fatalf("begin task dst: %v", err)
	}
	defer dst.EndTask(ctx, storage.TaskSummary{})

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("cksum job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	// mismatch branch
	os.WriteFile(filepath.Join(serveDir, "a.txt"), []byte("DIFFERENT"), 0o644)
	store2 := openTestStore(t)
	job2 := cksum.NewCksumJob(src, dst, store2,
		cksum.WithCksumParallelism(2),
		cksum.WithCksumDstFactory(dstFactory),
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

func TestIntegration_COSToRemote_Cksum_Resume(t *testing.T) {
	env := requireCOS(t)
	srcPrefix := newCOSPrefix(t, env, "cos2remote-cksum-r-src")
	serveDir := t.TempDir()

	files := map[string]string{
		"a.txt":        "alpha",
		"subdir/b.txt": "beta",
	}
	seedCOSPrefix(t, env, srcPrefix, files)
	for relPath, content := range files {
		path := filepath.Join(serveDir, relPath)
		os.MkdirAll(filepath.Dir(path), 0o755)
		os.WriteFile(path, []byte(content), 0o644)
	}
	// introduce mismatch
	os.WriteFile(filepath.Join(serveDir, "a.txt"), []byte("DIFFERENT"), 0o644)

	addr := startTestServer(t, serveDir)

	src := newCOSSource(t, env, srcPrefix)
	dst, err := remote.NewSource(addr, serveDir)
	if err != nil {
		t.Fatalf("new remote source: %v", err)
	}
	store := openTestStore(t)

	dstFactory := func(id int) (storage.Source, error) {
		return remote.NewSource(addr, serveDir)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	job := cksum.NewCksumJob(src, dst, store,
		cksum.WithCksumParallelism(2),
		cksum.WithCksumDstFactory(dstFactory),
		cksum.WithCksumAlgo(model.CksumMD5),
	)

	if err := dst.BeginTask(ctx, job.TaskID()); err != nil {
		t.Fatalf("begin task dst: %v", err)
	}

	exitCode, err := job.Run(ctx)
	if exitCode != 2 {
		t.Fatalf("expected exit code 2, got %d", exitCode)
	}
	if err == nil {
		t.Fatal("expected error for mismatch")
	}

	// fix the mismatch
	os.WriteFile(filepath.Join(serveDir, "a.txt"), []byte("alpha"), 0o644)

	job2 := cksum.NewCksumJob(src, dst, store,
		cksum.WithCksumResume(true),
		cksum.WithCksumTaskID(job.TaskID()),
		cksum.WithCksumParallelism(2),
		cksum.WithCksumDstFactory(dstFactory),
		cksum.WithCksumAlgo(model.CksumMD5),
	)

	exitCode, err = job2.Run(ctx)
	if err != nil {
		t.Fatalf("resume cksum job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0 on resume, got %d", exitCode)
	}

	dst.EndTask(ctx, storage.TaskSummary{})
}
