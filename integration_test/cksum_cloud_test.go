//go:build integration

package integration

import (
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zp001/ncp/internal/cksum"
	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/pkg/impls/storage/local"
	"github.com/zp001/ncp/pkg/impls/storage/remote"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// ========== Group 3: Cloud ComputeHash Verification (OSS/COS/OBS) ==========

// TestIntegration_OSS_ComputeHash verifies the three cksum paths for OSS:
//   - etag_md5: single-part upload → ETag-based hash → CksumPass
//   - ncp_md5_metadata: multipart upload with ncp-md5 → ncp-md5 path → CksumPass
//   - multipart_no_ncp_md5: raw SDK multipart without ncp-md5 → ErrChecksum → CksumMismatch
func TestIntegration_OSS_ComputeHash(t *testing.T) {
	env := requireOSS(t)

	t.Run("etag_md5", func(t *testing.T) {
		prefix := newOSSPrefix(t, env, "oss-cksum-etag")

		srcDir := t.TempDir()
		content := []byte("etag md5 checksum test")
		os.WriteFile(filepath.Join(srcDir, "file.txt"), content, 0o644)

		src, _ := local.NewSource(srcDir)
		dst := newOSSDestination(t, env, prefix)
		dstSrc := newOSSSource(t, env, prefix)
		store := openTestStore(t)

		job := copy.NewJob(src, dst, store,
			copy.WithParallelism(2),
			copy.WithCksumAlgo(model.CksumMD5),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		exitCode, err := job.Run(ctx)
		if err != nil || exitCode != 0 {
			t.Fatalf("copy job: exitCode=%d err=%v", exitCode, err)
		}

		cksumJob := cksum.NewCksumJob(src, dstSrc, store,
			cksum.WithCksumParallelism(2),
			cksum.WithCksumAlgo(model.CksumMD5),
		)

		exitCode, err = cksumJob.Run(ctx)
		if err != nil {
			t.Fatalf("cksum job: %v", err)
		}
		if exitCode != 0 {
			t.Fatalf("expected exit code 0 (CksumPass via ETag), got %d", exitCode)
		}
	})

	t.Run("ncp_md5_metadata", func(t *testing.T) {
		prefix := newOSSPrefix(t, env, "oss-cksum-ncpmd5")

		srcDir := t.TempDir()
		largeContent := make([]byte, testFileSize)
		for i := range largeContent {
			largeContent[i] = byte(i % 256)
		}
		os.WriteFile(filepath.Join(srcDir, "large.bin"), largeContent, 0o644)

		src, _ := local.NewSource(srcDir)
		dst := newOSSDestinationWithPartSize(t, env, prefix, testPartSize)
		dstSrc := newOSSSource(t, env, prefix)
		store := openTestStore(t)

		job := copy.NewJob(src, dst, store,
			copy.WithParallelism(2),
			copy.WithCksumAlgo(model.CksumMD5),
			copy.WithPartSize(testPartSize),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		exitCode, err := job.Run(ctx)
		if err != nil || exitCode != 0 {
			t.Fatalf("copy job: exitCode=%d err=%v", exitCode, err)
		}

		cksumJob := cksum.NewCksumJob(src, dstSrc, store,
			cksum.WithCksumParallelism(2),
			cksum.WithCksumAlgo(model.CksumMD5),
		)

		exitCode, err = cksumJob.Run(ctx)
		if err != nil {
			t.Fatalf("cksum job: %v", err)
		}
		if exitCode != 0 {
			t.Fatalf("expected exit code 0 (CksumPass via ncp-md5), got %d", exitCode)
		}
	})

	t.Run("multipart_no_ncp_md5", func(t *testing.T) {
		prefix := newOSSPrefix(t, env, "oss-cksum-nomd5")

		largeContent := make([]byte, testFileSize)
		for i := range largeContent {
			largeContent[i] = byte(i % 256)
		}

		uploadOSSMultipartNoNcpMD5(t, env, prefix, "large.bin", largeContent, testPartSize)

		srcDir := t.TempDir()
		os.WriteFile(filepath.Join(srcDir, "large.bin"), largeContent, 0o644)

		src, _ := local.NewSource(srcDir)
		dstSrc := newOSSSource(t, env, prefix)
		store := openTestStore(t)

		cksumJob := cksum.NewCksumJob(src, dstSrc, store,
			cksum.WithCksumParallelism(2),
			cksum.WithCksumAlgo(model.CksumMD5),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		exitCode, err := cksumJob.Run(ctx)
		if exitCode != 2 {
			t.Fatalf("expected exit code 2 (CksumMismatch via ErrChecksum), got %d", exitCode)
		}
		if err == nil {
			t.Fatal("expected error for mismatch")
		}
	})
}

func TestIntegration_COS_ComputeHash(t *testing.T) {
	env := requireCOS(t)

	t.Run("etag_md5", func(t *testing.T) {
		prefix := newCOSPrefix(t, env, "cos-cksum-etag")

		srcDir := t.TempDir()
		content := []byte("etag md5 checksum test")
		os.WriteFile(filepath.Join(srcDir, "file.txt"), content, 0o644)

		src, _ := local.NewSource(srcDir)
		dst := newCOSDestination(t, env, prefix)
		dstSrc := newCOSSource(t, env, prefix)
		store := openTestStore(t)

		job := copy.NewJob(src, dst, store,
			copy.WithParallelism(2),
			copy.WithCksumAlgo(model.CksumMD5),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		exitCode, err := job.Run(ctx)
		if err != nil || exitCode != 0 {
			t.Fatalf("copy job: exitCode=%d err=%v", exitCode, err)
		}

		cksumJob := cksum.NewCksumJob(src, dstSrc, store,
			cksum.WithCksumParallelism(2),
			cksum.WithCksumAlgo(model.CksumMD5),
		)

		exitCode, err = cksumJob.Run(ctx)
		if err != nil {
			t.Fatalf("cksum job: %v", err)
		}
		if exitCode != 0 {
			t.Fatalf("expected exit code 0 (CksumPass via ETag), got %d", exitCode)
		}
	})

	t.Run("ncp_md5_metadata", func(t *testing.T) {
		prefix := newCOSPrefix(t, env, "cos-cksum-ncpmd5")

		srcDir := t.TempDir()
		largeContent := make([]byte, testFileSize)
		for i := range largeContent {
			largeContent[i] = byte(i % 256)
		}
		os.WriteFile(filepath.Join(srcDir, "large.bin"), largeContent, 0o644)

		src, _ := local.NewSource(srcDir)
		dst := newCOSDestinationWithPartSize(t, env, prefix, testPartSize)
		dstSrc := newCOSSource(t, env, prefix)
		store := openTestStore(t)

		job := copy.NewJob(src, dst, store,
			copy.WithParallelism(2),
			copy.WithCksumAlgo(model.CksumMD5),
			copy.WithPartSize(testPartSize),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		exitCode, err := job.Run(ctx)
		if err != nil || exitCode != 0 {
			t.Fatalf("copy job: exitCode=%d err=%v", exitCode, err)
		}

		cksumJob := cksum.NewCksumJob(src, dstSrc, store,
			cksum.WithCksumParallelism(2),
			cksum.WithCksumAlgo(model.CksumMD5),
		)

		exitCode, err = cksumJob.Run(ctx)
		if err != nil {
			t.Fatalf("cksum job: %v", err)
		}
		if exitCode != 0 {
			t.Fatalf("expected exit code 0 (CksumPass via ncp-md5), got %d", exitCode)
		}
	})

	t.Run("multipart_no_ncp_md5", func(t *testing.T) {
		prefix := newCOSPrefix(t, env, "cos-cksum-nomd5")

		largeContent := make([]byte, testFileSize)
		for i := range largeContent {
			largeContent[i] = byte(i % 256)
		}

		uploadCOSMultipartNoNcpMD5(t, env, prefix, "large.bin", largeContent, testPartSize)

		srcDir := t.TempDir()
		os.WriteFile(filepath.Join(srcDir, "large.bin"), largeContent, 0o644)

		src, _ := local.NewSource(srcDir)
		dstSrc := newCOSSource(t, env, prefix)
		store := openTestStore(t)

		cksumJob := cksum.NewCksumJob(src, dstSrc, store,
			cksum.WithCksumParallelism(2),
			cksum.WithCksumAlgo(model.CksumMD5),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		exitCode, err := cksumJob.Run(ctx)
		if exitCode != 2 {
			t.Fatalf("expected exit code 2 (CksumMismatch via ErrChecksum), got %d", exitCode)
		}
		if err == nil {
			t.Fatal("expected error for mismatch")
		}
	})
}

func TestIntegration_OBS_ComputeHash(t *testing.T) {
	env := requireOBS(t)

	t.Run("etag_md5", func(t *testing.T) {
		prefix := newOBSPrefix(t, env, "obs-cksum-etag")

		srcDir := t.TempDir()
		content := []byte("etag md5 checksum test")
		os.WriteFile(filepath.Join(srcDir, "file.txt"), content, 0o644)

		src, _ := local.NewSource(srcDir)
		dst := newOBSDestination(t, env, prefix)
		dstSrc := newOBSSource(t, env, prefix)
		store := openTestStore(t)

		job := copy.NewJob(src, dst, store,
			copy.WithParallelism(2),
			copy.WithCksumAlgo(model.CksumMD5),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		exitCode, err := job.Run(ctx)
		if err != nil || exitCode != 0 {
			t.Fatalf("copy job: exitCode=%d err=%v", exitCode, err)
		}

		cksumJob := cksum.NewCksumJob(src, dstSrc, store,
			cksum.WithCksumParallelism(2),
			cksum.WithCksumAlgo(model.CksumMD5),
		)

		exitCode, err = cksumJob.Run(ctx)
		if err != nil {
			t.Fatalf("cksum job: %v", err)
		}
		if exitCode != 0 {
			t.Fatalf("expected exit code 0 (CksumPass via ETag), got %d", exitCode)
		}
	})

	t.Run("ncp_md5_metadata", func(t *testing.T) {
		prefix := newOBSPrefix(t, env, "obs-cksum-ncpmd5")

		srcDir := t.TempDir()
		largeContent := make([]byte, testFileSize)
		for i := range largeContent {
			largeContent[i] = byte(i % 256)
		}
		os.WriteFile(filepath.Join(srcDir, "large.bin"), largeContent, 0o644)

		src, _ := local.NewSource(srcDir)
		dst := newOBSDestinationWithPartSize(t, env, prefix, testPartSize)
		dstSrc := newOBSSource(t, env, prefix)
		store := openTestStore(t)

		job := copy.NewJob(src, dst, store,
			copy.WithParallelism(2),
			copy.WithCksumAlgo(model.CksumMD5),
			copy.WithPartSize(testPartSize),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		exitCode, err := job.Run(ctx)
		if err != nil || exitCode != 0 {
			t.Fatalf("copy job: exitCode=%d err=%v", exitCode, err)
		}

		cksumJob := cksum.NewCksumJob(src, dstSrc, store,
			cksum.WithCksumParallelism(2),
			cksum.WithCksumAlgo(model.CksumMD5),
		)

		exitCode, err = cksumJob.Run(ctx)
		if err != nil {
			t.Fatalf("cksum job: %v", err)
		}
		if exitCode != 0 {
			t.Fatalf("expected exit code 0 (CksumPass via ncp-md5), got %d", exitCode)
		}
	})

	t.Run("multipart_no_ncp_md5", func(t *testing.T) {
		prefix := newOBSPrefix(t, env, "obs-cksum-nomd5")

		largeContent := make([]byte, testFileSize)
		for i := range largeContent {
			largeContent[i] = byte(i % 256)
		}

		uploadOBSMultipartNoNcpMD5(t, env, prefix, "large.bin", largeContent, testPartSize)

		srcDir := t.TempDir()
		os.WriteFile(filepath.Join(srcDir, "large.bin"), largeContent, 0o644)

		src, _ := local.NewSource(srcDir)
		dstSrc := newOBSSource(t, env, prefix)
		store := openTestStore(t)

		cksumJob := cksum.NewCksumJob(src, dstSrc, store,
			cksum.WithCksumParallelism(2),
			cksum.WithCksumAlgo(model.CksumMD5),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		exitCode, err := cksumJob.Run(ctx)
		if exitCode != 2 {
			t.Fatalf("expected exit code 2 (CksumMismatch via ErrChecksum), got %d", exitCode)
		}
		if err == nil {
			t.Fatal("expected error for mismatch")
		}
	})
}

// ========== Group 4: Hash-only Protocol Correctness ==========

// TestIntegration_RemoteCksum_HashProtocol verifies the ncp:// hash protocol
// by directly calling ComputeHash on remote sources.
func TestIntegration_RemoteCksum_HashProtocol(t *testing.T) {
	t.Run("multi_chunk", func(t *testing.T) {
		serveDir := t.TempDir()
		fileSize := 3 * int64(storage.CksumChunkSize) // 3MB = 3 chunks
		content := make([]byte, fileSize)
		for i := range content {
			content[i] = byte(i % 256)
		}
		os.WriteFile(filepath.Join(serveDir, "multi.bin"), content, 0o644)

		expectedWholeHash := computeMD5Hex(content)
		expectedChunkCount := int(fileSize / storage.CksumChunkSize)

		addr := startTestServer(t, serveDir)
		src, err := remote.NewSource(addr, serveDir, remote.WithConfigJSON("{}"))
		if err != nil {
			t.Fatalf("remote.NewSource: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := src.BeginTask(ctx, "test-hash-protocol"); err != nil {
			t.Fatalf("BeginTask: %v", err)
		}
		defer src.EndTask(ctx, storage.TaskSummary{})

		result, err := src.ComputeHash(ctx, "multi.bin", model.CksumMD5, storage.CksumChunkSize)
		if err != nil {
			t.Fatalf("ComputeHash: %v", err)
		}

		if result.WholeFileHash != expectedWholeHash {
			t.Errorf("WholeFileHash: got %q, want %q", result.WholeFileHash, expectedWholeHash)
		}
		if len(result.ChunkHashes) != expectedChunkCount {
			t.Errorf("ChunkHashes count: got %d, want %d", len(result.ChunkHashes), expectedChunkCount)
		}

		// Verify individual chunk hashes
		chunkHasher := md5.New()
		for i, chunkHash := range result.ChunkHashes {
			start := int64(i) * storage.CksumChunkSize
			end := start + storage.CksumChunkSize
			if end > fileSize {
				end = fileSize
			}
			chunkHasher.Reset()
			chunkHasher.Write(content[start:end])
			expected := fmt.Sprintf("%x", chunkHasher.Sum(nil))
			if chunkHash != expected {
				t.Errorf("ChunkHash[%d]: got %q, want %q", i, chunkHash, expected)
			}
		}
	})

	t.Run("xxh64", func(t *testing.T) {
		serveDir := t.TempDir()
		fileSize := 3 * int64(storage.CksumChunkSize)
		content := make([]byte, fileSize)
		for i := range content {
			content[i] = byte(i % 256)
		}
		os.WriteFile(filepath.Join(serveDir, "xxh64.bin"), content, 0o644)

		addr := startTestServer(t, serveDir)
		src, err := remote.NewSource(addr, serveDir, remote.WithConfigJSON("{}"))
		if err != nil {
			t.Fatalf("remote.NewSource: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := src.BeginTask(ctx, "test-hash-xxh64"); err != nil {
			t.Fatalf("BeginTask: %v", err)
		}
		defer src.EndTask(ctx, storage.TaskSummary{})

		result, err := src.ComputeHash(ctx, "xxh64.bin", model.CksumXXH64, storage.CksumChunkSize)
		if err != nil {
			t.Fatalf("ComputeHash xxh64: %v", err)
		}

		if result.Algo != "xxh64" {
			t.Errorf("Algo: got %q, want %q", result.Algo, "xxh64")
		}

		expectedHasher := model.NewHasher(model.CksumXXH64)
		expectedHasher.Write(content)
		expectedWhole := model.SumToHex(expectedHasher)
		if result.WholeFileHash != expectedWhole {
			t.Errorf("WholeFileHash: got %q, want %q", result.WholeFileHash, expectedWhole)
		}

		if len(result.ChunkHashes) != 3 {
			t.Errorf("ChunkHashes count: got %d, want 3", len(result.ChunkHashes))
		}
	})

	t.Run("empty_file", func(t *testing.T) {
		serveDir := t.TempDir()
		os.WriteFile(filepath.Join(serveDir, "empty.bin"), []byte{}, 0o644)

		expectedHash := "d41d8cd98f00b204e9800998ecf8427e"

		addr := startTestServer(t, serveDir)
		src, err := remote.NewSource(addr, serveDir, remote.WithConfigJSON("{}"))
		if err != nil {
			t.Fatalf("remote.NewSource: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := src.BeginTask(ctx, "test-hash-empty"); err != nil {
			t.Fatalf("BeginTask: %v", err)
		}
		defer src.EndTask(ctx, storage.TaskSummary{})

		result, err := src.ComputeHash(ctx, "empty.bin", model.CksumMD5, storage.CksumChunkSize)
		if err != nil {
			t.Fatalf("ComputeHash empty: %v", err)
		}

		if result.WholeFileHash != expectedHash {
			t.Errorf("WholeFileHash: got %q, want %q", result.WholeFileHash, expectedHash)
		}
		// Remote protocol sends 1 result for empty file (EOF on first chunk read)
		if len(result.ChunkHashes) != 1 {
			t.Errorf("ChunkHashes count: got %d, want 1", len(result.ChunkHashes))
		}
		if len(result.ChunkHashes) > 0 && result.ChunkHashes[0] != expectedHash {
			t.Errorf("ChunkHash[0]: got %q, want %q", result.ChunkHashes[0], expectedHash)
		}
	})
}

// ========== Group 5: Per-chunk Mismatch Detection ==========

// TestIntegration_Cksum_ChunkMismatch verifies that cksum detects mismatches
// at the chunk level and identifies which chunk differs.
func TestIntegration_Cksum_ChunkMismatch(t *testing.T) {
	fileSize := 3 * storage.CksumChunkSize // 3MB = 3 chunks
	originalContent := make([]byte, fileSize)
	for i := range originalContent {
		originalContent[i] = byte(i % 256)
	}

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	os.WriteFile(filepath.Join(srcDir, "data.bin"), originalContent, 0o644)

	// Modify chunk 1 (bytes 1MB-2MB) in dst
	modifiedContent := make([]byte, fileSize)
	for i := range originalContent {
		modifiedContent[i] = originalContent[i]
	}
	for i := storage.CksumChunkSize; i < 2*storage.CksumChunkSize; i++ {
		modifiedContent[i] = byte((i + 1) % 256)
	}
	os.WriteFile(filepath.Join(dstDir, "data.bin"), modifiedContent, 0o644)

	src, _ := local.NewSource(srcDir)
	dst, _ := local.NewSource(dstDir)
	store := openTestStore(t)

	cksumJob := cksum.NewCksumJob(src, dst, store,
		cksum.WithCksumParallelism(2),
		cksum.WithCksumAlgo(model.CksumMD5),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	exitCode, err := cksumJob.Run(ctx)
	if exitCode != 2 {
		t.Fatalf("expected exitCode 2 (CksumMismatch), got %d", exitCode)
	}
	if err == nil {
		t.Fatal("expected error for mismatch")
	}

	// Verify CksumMismatch is recorded in DB for the file
	_, cksumStatus, dbErr := store.Get("data.bin")
	if dbErr != nil {
		t.Fatalf("store.Get: %v", dbErr)
	}
	if cksumStatus != model.CksumMismatch {
		t.Errorf("CksumStatus: got %s, want %s", cksumStatus, model.CksumMismatch)
	}

	// Verify chunk-level mismatch location via direct ComputeHash
	srcResult, srcErr := src.ComputeHash(ctx, "data.bin", model.CksumMD5, storage.CksumChunkSize)
	if srcErr != nil {
		t.Fatalf("src ComputeHash: %v", srcErr)
	}
	dstResult, dstErr := dst.ComputeHash(ctx, "data.bin", model.CksumMD5, storage.CksumChunkSize)
	if dstErr != nil {
		t.Fatalf("dst ComputeHash: %v", dstErr)
	}

	if srcResult.WholeFileHash == dstResult.WholeFileHash {
		t.Error("whole-file hashes should differ after chunk modification")
	}

	if len(srcResult.ChunkHashes) != 3 {
		t.Errorf("src chunk count: got %d, want 3", len(srcResult.ChunkHashes))
	}
	if len(dstResult.ChunkHashes) != 3 {
		t.Errorf("dst chunk count: got %d, want 3", len(dstResult.ChunkHashes))
	}

	// Chunk 0 and 2 should match; chunk 1 should mismatch
	if len(srcResult.ChunkHashes) >= 3 && len(dstResult.ChunkHashes) >= 3 {
		if srcResult.ChunkHashes[0] != dstResult.ChunkHashes[0] {
			t.Errorf("chunk 0 should match: src=%s dst=%s", srcResult.ChunkHashes[0], dstResult.ChunkHashes[0])
		}
		if srcResult.ChunkHashes[1] == dstResult.ChunkHashes[1] {
			t.Error("chunk 1 should mismatch after modification")
		}
		if srcResult.ChunkHashes[2] != dstResult.ChunkHashes[2] {
			t.Errorf("chunk 2 should match: src=%s dst=%s", srcResult.ChunkHashes[2], dstResult.ChunkHashes[2])
		}
	}

	// Verify the cksum job error mentions chunk-level mismatch
	if !strings.Contains(err.Error(), "mismatch") {
		t.Errorf("error should mention mismatch, got: %s", err.Error())
	}
}