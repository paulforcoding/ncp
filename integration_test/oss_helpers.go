//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/zp001/ncp/pkg/impls/storage/aliyun"
	"github.com/zp001/ncp/pkg/interfaces/storage"
)

// ossEnv holds OSS credentials and endpoint info.
type ossEnv struct {
	Endpoint string
	Region   string
	AK       string
	SK       string
	Bucket   string
}

// requireOSS skips the test if OSS credentials are not available.
func requireOSS(t *testing.T) ossEnv {
	t.Helper()
	ak := os.Getenv("NCP_OSS_AK")
	sk := os.Getenv("NCP_OSS_SK")
	if ak == "" || sk == "" {
		t.Skip("NCP_OSS_AK / NCP_OSS_SK not set, skipping OSS integration test")
	}
	env := ossEnv{
		Endpoint: os.Getenv("NCP_OSS_ENDPOINT"),
		Region:   os.Getenv("NCP_OSS_REGION"),
		AK:       ak,
		SK:       sk,
		Bucket:   os.Getenv("NCP_OSS_BUCKET"),
	}
	if env.Endpoint == "" {
		env.Endpoint = "oss-cn-shenzhen.aliyuncs.com"
	}
	if env.Region == "" {
		env.Region = "cn-shenzhen"
	}
	if env.Bucket == "" {
		env.Bucket = "ncpbucket1"
	}
	return env
}

// newOSSPrefix returns a unique prefix and registers cleanup.
func newOSSPrefix(t *testing.T, env ossEnv, label string) string {
	t.Helper()
	prefix := fmt.Sprintf("ncp-it/%s/%d/", label, time.Now().UnixNano())
	t.Cleanup(func() { cleanupOSSPrefix(t, env, prefix) })
	return prefix
}

// cleanupOSSPrefix deletes all objects under prefix.
func cleanupOSSPrefix(t *testing.T, env ossEnv, prefix string) {
	t.Helper()
	client := newOSSClient(env)
	ctx := context.Background()
	p := client.NewListObjectsV2Paginator(&oss.ListObjectsV2Request{
		Bucket: oss.Ptr(env.Bucket),
		Prefix: oss.Ptr(prefix),
	})
	for p.HasNext() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Logf("cleanup list error: %v", err)
			return
		}
		for _, obj := range page.Contents {
			_, _ = client.DeleteObject(ctx, &oss.DeleteObjectRequest{
				Bucket: oss.Ptr(env.Bucket),
				Key:    obj.Key,
			})
		}
	}
}

// newOSSClient creates a raw OSS SDK client from env.
func newOSSClient(env ossEnv) *oss.Client {
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewStaticCredentialsProvider(env.AK, env.SK)).
		WithRegion(env.Region).
		WithEndpoint(env.Endpoint)
	return oss.NewClient(cfg)
}

// newOSSSource creates an ncp aliyun.Source for testing.
func newOSSSource(t *testing.T, env ossEnv, prefix string) *aliyun.Source {
	t.Helper()
	src, err := aliyun.NewSource(aliyun.SourceConfig{
		Endpoint: env.Endpoint,
		Region:   env.Region,
		AK:       env.AK,
		SK:       env.SK,
		Bucket:   env.Bucket,
		Prefix:   prefix,
	})
	if err != nil {
		t.Fatalf("aliyun.NewSource: %v", err)
	}
	return src
}

// newOSSDestination creates an ncp aliyun.Destination for testing.
func newOSSDestination(t *testing.T, env ossEnv, prefix string) *aliyun.Destination {
	t.Helper()
	dst, err := aliyun.NewDestination(aliyun.Config{
		Endpoint: env.Endpoint,
		Region:   env.Region,
		AK:       env.AK,
		SK:       env.SK,
		Bucket:   env.Bucket,
		Prefix:   prefix,
	})
	if err != nil {
		t.Fatalf("aliyun.NewDestination: %v", err)
	}
	return dst
}

// seedOSSPrefix uploads a map of relative paths to content under the given prefix.
// Also creates directory marker objects and POSIX metadata for cksum/copy tests.
func seedOSSPrefix(t *testing.T, env ossEnv, prefix string, files map[string]string) {
	t.Helper()
	client := newOSSClient(env)
	ctx := context.Background()

	// Track created dirs to avoid duplicate PutObject calls
	createdDirs := make(map[string]bool)

	for relPath, content := range files {
		key := prefix + relPath

		// Create parent directory markers
		dir := filepath.Dir(relPath)
		for dir != "." && dir != "/" {
			dirKey := prefix + dir + "/"
			if !createdDirs[dirKey] {
				createdDirs[dirKey] = true
				_, err := client.PutObject(ctx, &oss.PutObjectRequest{
					Bucket:   oss.Ptr(env.Bucket),
					Key:      oss.Ptr(dirKey),
					Body:     strings.NewReader(""),
					Metadata: map[string]string{"ncp-mode": "0755"},
				})
				if err != nil {
					t.Fatalf("PutObject dir marker %s: %v", dirKey, err)
				}
			}
			dir = filepath.Dir(dir)
		}

		_, err := client.PutObject(ctx, &oss.PutObjectRequest{
			Bucket:   oss.Ptr(env.Bucket),
			Key:      oss.Ptr(key),
			Body:     strings.NewReader(content),
			Metadata: map[string]string{"ncp-mode": "0644"},
		})
		if err != nil {
			t.Fatalf("PutObject %s: %v", key, err)
		}
	}
}

// verifyOSSPrefix verifies that objects under prefix match expected content.
func verifyOSSPrefix(t *testing.T, env ossEnv, prefix string, expected map[string]string) {
	t.Helper()
	client := newOSSClient(env)
	ctx := context.Background()
	for relPath, want := range expected {
		key := prefix + relPath
		result, err := client.GetObject(ctx, &oss.GetObjectRequest{
			Bucket: oss.Ptr(env.Bucket),
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
		if string(data) != want {
			t.Errorf("content mismatch %s: got %q, want %q", relPath, string(data), want)
		}
	}
}

// putOSSObject overwrites a single object with new content.
func putOSSObject(t *testing.T, env ossEnv, prefix, relPath, content string) {
	t.Helper()
	client := newOSSClient(env)
	key := prefix + relPath
	_, err := client.PutObject(context.Background(), &oss.PutObjectRequest{
		Bucket:   oss.Ptr(env.Bucket),
		Key:      oss.Ptr(key),
		Body:     strings.NewReader(content),
		Metadata: map[string]string{"ncp-mode": "0644"},
	})
	if err != nil {
		t.Fatalf("PutObject %s: %v", key, err)
	}
}

// failAfterNShared wraps a Destination and makes OpenFile fail after N calls globally.
type failAfterNShared struct {
	storage.Destination
	mu     *sync.Mutex
	count  *int
	failAt int
}

func (d *failAfterNShared) OpenFile(relPath string, size int64, mode os.FileMode, uid, gid int) (storage.Writer, error) {
	d.mu.Lock()
	*d.count++
	if *d.count > d.failAt {
		d.mu.Unlock()
		return nil, fmt.Errorf("simulated error after %d files", d.failAt)
	}
	d.mu.Unlock()
	return d.Destination.OpenFile(relPath, size, mode, uid, gid)
}

// newFailAfterNShared creates a shared counter failAfterN wrapper.
func newFailAfterNShared(dst storage.Destination, failAt int) *failAfterNShared {
	return &failAfterNShared{
		Destination: dst,
		mu:          &sync.Mutex{},
		count:       new(int),
		failAt:      failAt,
	}
}

// failAfterNOpenShared wraps a Source and makes Open fail after N calls globally.
type failAfterNOpenShared struct {
	storage.Source
	mu     *sync.Mutex
	count  *int
	failAt int
}

func (s *failAfterNOpenShared) Open(relPath string) (storage.Reader, error) {
	s.mu.Lock()
	*s.count++
	if *s.count > s.failAt {
		s.mu.Unlock()
		return nil, fmt.Errorf("simulated open error after %d files", s.failAt)
	}
	s.mu.Unlock()
	return s.Source.Open(relPath)
}

func newFailAfterNOpenShared(src storage.Source, failAt int) *failAfterNOpenShared {
	return &failAfterNOpenShared{
		Source: src,
		mu:     &sync.Mutex{},
		count:  new(int),
		failAt: failAt,
	}
}

// countFilesOSS counts objects under prefix (treating trailing-/ keys as dirs).
func countFilesOSS(t *testing.T, env ossEnv, prefix string) (regulars int, dirs int) {
	t.Helper()
	client := newOSSClient(env)
	ctx := context.Background()
	p := client.NewListObjectsV2Paginator(&oss.ListObjectsV2Request{
		Bucket: oss.Ptr(env.Bucket),
		Prefix: oss.Ptr(prefix),
	})
	for p.HasNext() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Logf("list error: %v", err)
			return
		}
		for _, obj := range page.Contents {
			key := oss.ToString(obj.Key)
			relPath := strings.TrimPrefix(key, prefix)
			if relPath == "" {
				continue
			}
			if strings.HasSuffix(key, "/") {
				dirs++
			} else {
				regulars++
			}
		}
	}
	return
}
