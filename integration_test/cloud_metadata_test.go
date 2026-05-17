//go:build integration

package integration

import (
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zp001/ncp/internal/copy"
	"github.com/zp001/ncp/pkg/impls/storage/local"
	"github.com/zp001/ncp/pkg/model"
)

// computeMD5Hex computes the hex-encoded MD5 of data.
func computeMD5Hex(data []byte) string {
	h := md5.New()
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// ========== Group 1: MetadataRoundTrip (small file, single-part upload) ==========

func TestIntegration_OSS_MetadataRoundTrip(t *testing.T) {
	env := requireOSS(t)
	dstPrefix := newOSSPrefix(t, env, "oss-meta-rt")

	srcDir := t.TempDir()
	content := []byte("metadata round trip test")
	os.WriteFile(filepath.Join(srcDir, "file.txt"), content, 0o644)

	src, _ := local.NewSource(srcDir)
	dst := newOSSDestination(t, env, dstPrefix)
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

	meta := headOSSObjectMetadata(t, env, dstPrefix, "file.txt")
	expectedMD5 := computeMD5Hex(content)
	if meta.Metadata["ncp-md5"] != expectedMD5 {
		t.Errorf("ncp-md5: got %q, want %q", meta.Metadata["ncp-md5"], expectedMD5)
	}
	// Single-part upload should NOT have ncp-part-size
	if _, ok := meta.Metadata["ncp-part-size"]; ok {
		t.Errorf("ncp-part-size should not exist for single-part upload, got %q", meta.Metadata["ncp-part-size"])
	}
}

func TestIntegration_COS_MetadataRoundTrip(t *testing.T) {
	env := requireCOS(t)
	dstPrefix := newCOSPrefix(t, env, "cos-meta-rt")

	srcDir := t.TempDir()
	content := []byte("metadata round trip test")
	os.WriteFile(filepath.Join(srcDir, "file.txt"), content, 0o644)

	src, _ := local.NewSource(srcDir)
	dst := newCOSDestination(t, env, dstPrefix)
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

	meta := headCOSObjectMetadata(t, env, dstPrefix, "file.txt")
	expectedMD5 := computeMD5Hex(content)
	// COS stores custom metadata with "ncp-" prefix, but the SDK returns them
	// as x-cos-meta-ncp-md5 headers. Our helper strips "x-cos-meta-".
	if meta.Metadata["ncp-md5"] != expectedMD5 {
		t.Errorf("ncp-md5: got %q, want %q", meta.Metadata["ncp-md5"], expectedMD5)
	}
	if _, ok := meta.Metadata["ncp-part-size"]; ok {
		t.Errorf("ncp-part-size should not exist for single-part upload, got %q", meta.Metadata["ncp-part-size"])
	}
}

func TestIntegration_OBS_MetadataRoundTrip(t *testing.T) {
	env := requireOBS(t)
	dstPrefix := newOBSPrefix(t, env, "obs-meta-rt")

	srcDir := t.TempDir()
	content := []byte("metadata round trip test")
	os.WriteFile(filepath.Join(srcDir, "file.txt"), content, 0o644)

	src, _ := local.NewSource(srcDir)
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

	meta := headOBSObjectMetadata(t, env, dstPrefix, "file.txt")
	expectedMD5 := computeMD5Hex(content)
	if meta.Metadata["ncp-md5"] != expectedMD5 {
		t.Errorf("ncp-md5: got %q, want %q", meta.Metadata["ncp-md5"], expectedMD5)
	}
	if _, ok := meta.Metadata["ncp-part-size"]; ok {
		t.Errorf("ncp-part-size should not exist for single-part upload, got %q", meta.Metadata["ncp-part-size"])
	}
}

// ========== Group 2: MultipartMetadata (large file, lowered part-size) ==========

// Use 5MB part-size (minimum allowed by cloud providers) to trigger multipart
// with a 10MB file, avoiding the cost of uploading a real 100MB file.

const testPartSize = 5 << 20 // 5MB
const testFileSize = 10 << 20 // 10MB → 2 parts with 5MB part-size

func TestIntegration_OSS_MultipartMetadata(t *testing.T) {
	env := requireOSS(t)
	dstPrefix := newOSSPrefix(t, env, "oss-mp-meta")

	srcDir := t.TempDir()
	largeContent := make([]byte, testFileSize)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}
	os.WriteFile(filepath.Join(srcDir, "large.bin"), largeContent, 0o644)

	src, _ := local.NewSource(srcDir)
	dst := newOSSDestinationWithPartSize(t, env, dstPrefix, testPartSize)
	store := openTestStore(t)

	job := copy.NewJob(src, dst, store,
		copy.WithParallelism(2),
		copy.WithCksumAlgo(model.CksumMD5),
		copy.WithPartSize(testPartSize),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	meta := headOSSObjectMetadata(t, env, dstPrefix, "large.bin")
	expectedMD5 := computeMD5Hex(largeContent)

	// Verify ncp-md5 matches actual MD5 of file content
	if meta.Metadata["ncp-md5"] != expectedMD5 {
		t.Errorf("ncp-md5: got %q, want %q", meta.Metadata["ncp-md5"], expectedMD5)
	}

	// Verify ncp-part-size matches the configured part-size
	if meta.Metadata["ncp-part-size"] != fmt.Sprintf("%d", testPartSize) {
		t.Errorf("ncp-part-size: got %q, want %q", meta.Metadata["ncp-part-size"], fmt.Sprintf("%d", testPartSize))
	}

	// Verify Content-Length matches file size
	if meta.ContentLength != testFileSize {
		t.Errorf("ContentLength: got %d, want %d", meta.ContentLength, testFileSize)
	}
}

func TestIntegration_COS_MultipartMetadata(t *testing.T) {
	env := requireCOS(t)
	dstPrefix := newCOSPrefix(t, env, "cos-mp-meta")

	srcDir := t.TempDir()
	largeContent := make([]byte, testFileSize)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}
	os.WriteFile(filepath.Join(srcDir, "large.bin"), largeContent, 0o644)

	src, _ := local.NewSource(srcDir)
	dst := newCOSDestinationWithPartSize(t, env, dstPrefix, testPartSize)
	store := openTestStore(t)

	job := copy.NewJob(src, dst, store,
		copy.WithParallelism(2),
		copy.WithCksumAlgo(model.CksumMD5),
		copy.WithPartSize(testPartSize),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	meta := headCOSObjectMetadata(t, env, dstPrefix, "large.bin")
	expectedMD5 := computeMD5Hex(largeContent)

	if meta.Metadata["ncp-md5"] != expectedMD5 {
		t.Errorf("ncp-md5: got %q, want %q", meta.Metadata["ncp-md5"], expectedMD5)
	}
	if meta.Metadata["ncp-part-size"] != fmt.Sprintf("%d", testPartSize) {
		t.Errorf("ncp-part-size: got %q, want %q", meta.Metadata["ncp-part-size"], fmt.Sprintf("%d", testPartSize))
	}
	if meta.ContentLength != testFileSize {
		t.Errorf("ContentLength: got %d, want %d", meta.ContentLength, testFileSize)
	}
}

func TestIntegration_OBS_MultipartMetadata(t *testing.T) {
	env := requireOBS(t)
	dstPrefix := newOBSPrefix(t, env, "obs-mp-meta")

	srcDir := t.TempDir()
	largeContent := make([]byte, testFileSize)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}
	os.WriteFile(filepath.Join(srcDir, "large.bin"), largeContent, 0o644)

	src, _ := local.NewSource(srcDir)
	dst := newOBSDestinationWithPartSize(t, env, dstPrefix, testPartSize)
	store := openTestStore(t)

	job := copy.NewJob(src, dst, store,
		copy.WithParallelism(2),
		copy.WithCksumAlgo(model.CksumMD5),
		copy.WithPartSize(testPartSize),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	meta := headOBSObjectMetadata(t, env, dstPrefix, "large.bin")
	expectedMD5 := computeMD5Hex(largeContent)

	if meta.Metadata["ncp-md5"] != expectedMD5 {
		t.Errorf("ncp-md5: got %q, want %q", meta.Metadata["ncp-md5"], expectedMD5)
	}
	if meta.Metadata["ncp-part-size"] != fmt.Sprintf("%d", testPartSize) {
		t.Errorf("ncp-part-size: got %q, want %q", meta.Metadata["ncp-part-size"], fmt.Sprintf("%d", testPartSize))
	}
	if meta.ContentLength != testFileSize {
		t.Errorf("ContentLength: got %d, want %d", meta.ContentLength, testFileSize)
	}
}