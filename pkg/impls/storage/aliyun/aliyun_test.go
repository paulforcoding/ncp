//go:build integration

package aliyun

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
)

const (
	testAK       = "LTAI5t***FAKE***AK"
	testSK       = "FAKE***SK***PLACEHOLDER"
	testEndpoint = "oss-cn-shenzhen.aliyuncs.com"
	testBucket   = "ncpbucket1"
	testRegion   = "cn-shenzhen"
)

func newTestClient(t *testing.T) *oss.Client {
	t.Helper()
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewStaticCredentialsProvider(testAK, testSK)).
		WithRegion(testRegion).
		WithEndpoint(testEndpoint)
	return oss.NewClient(cfg)
}

func TestIntegration_PutGetList(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	prefix := fmt.Sprintf("ncp-test/%d/", time.Now().UnixMilli())

	// Upload 3 objects
	contents := map[string]string{
		prefix + "hello.txt":     "hello oss",
		prefix + "subdir/a.txt":  "file a",
		prefix + "subdir/b.txt":  "file b",
	}

	keys := make([]string, 0, len(contents))
	for key, content := range contents {
		_, err := client.PutObject(ctx, &oss.PutObjectRequest{
			Bucket: oss.Ptr(testBucket),
			Key:    oss.Ptr(key),
			Body:   strings.NewReader(content),
		})
		if err != nil {
			t.Fatalf("PutObject %s: %v", key, err)
		}
		keys = append(keys, key)
		t.Logf("uploaded: %s", key)
	}

	// Download and verify each object
	for key, wantContent := range contents {
		result, err := client.GetObject(ctx, &oss.GetObjectRequest{
			Bucket: oss.Ptr(testBucket),
			Key:    oss.Ptr(key),
		})
		if err != nil {
			t.Fatalf("GetObject %s: %v", key, err)
		}
		data, err := io.ReadAll(result.Body)
		result.Body.Close()
		if err != nil {
			t.Fatalf("read body %s: %v", key, err)
		}
		if string(data) != wantContent {
			t.Errorf("content mismatch for %s: got %q, want %q", key, string(data), wantContent)
		}
		t.Logf("downloaded: %s (%d bytes)", key, len(data))
	}

	// List objects under prefix
	p := client.NewListObjectsV2Paginator(&oss.ListObjectsV2Request{
		Bucket: oss.Ptr(testBucket),
		Prefix: oss.Ptr(prefix),
	})

	var listedKeys []string
	for p.HasNext() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Fatalf("ListObjectsV2: %v", err)
		}
		for _, obj := range page.Contents {
			listedKeys = append(listedKeys, oss.ToString(obj.Key))
		}
	}

	sort.Strings(keys)
	sort.Strings(listedKeys)
	if len(keys) != len(listedKeys) {
		t.Errorf("listed %d objects, want %d", len(listedKeys), len(keys))
	}
	for i, k := range keys {
		if i >= len(listedKeys) || listedKeys[i] != k {
			t.Errorf("list mismatch at %d: got %q, want %q", i, listedKeys[i], k)
		}
	}
	t.Logf("listed %d objects under %s", len(listedKeys), prefix)

	// Clean up
	for _, key := range keys {
		_, err := client.DeleteObject(ctx, &oss.DeleteObjectRequest{
			Bucket: oss.Ptr(testBucket),
			Key:    oss.Ptr(key),
		})
		if err != nil {
			t.Logf("warning: delete %s: %v", key, err)
		}
	}
}

func TestIntegration_LargeFileUploadDownload(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	key := fmt.Sprintf("ncp-test/large-%d.bin", time.Now().UnixMilli())

	// Generate 10MB of data with known content
	size := 10 << 20 // 10MB
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251) // prime modulus for pseudo-random pattern
	}
	wantMD5 := fmt.Sprintf("%x", md5.Sum(data))

	// Upload
	_, err := client.PutObject(ctx, &oss.PutObjectRequest{
		Bucket: oss.Ptr(testBucket),
		Key:    oss.Ptr(key),
		Body:   strings.NewReader(string(data)),
	})
	if err != nil {
		t.Fatalf("PutObject large file: %v", err)
	}
	t.Logf("uploaded: %s (%d bytes)", key, size)

	// Download
	result, err := client.GetObject(ctx, &oss.GetObjectRequest{
		Bucket: oss.Ptr(testBucket),
		Key:    oss.Ptr(key),
	})
	if err != nil {
		t.Fatalf("GetObject large file: %v", err)
	}
	gotData, err := io.ReadAll(result.Body)
	result.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	gotMD5 := fmt.Sprintf("%x", md5.Sum(gotData))
	if gotMD5 != wantMD5 {
		t.Errorf("MD5 mismatch: got %s, want %s", gotMD5, wantMD5)
	}
	t.Logf("downloaded: %s (%d bytes, md5=%s)", key, len(gotData), gotMD5[:16])

	// Clean up
	_, _ = client.DeleteObject(ctx, &oss.DeleteObjectRequest{
		Bucket: oss.Ptr(testBucket),
		Key:    oss.Ptr(key),
	})
}
