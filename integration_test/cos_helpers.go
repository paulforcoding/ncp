//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tencentyun/cos-go-sdk-v5"
	cosbackend "github.com/zp001/ncp/pkg/impls/storage/cos"
)

// cosEnv holds COS credentials and endpoint info.
type cosEnv struct {
	Endpoint string // optional custom endpoint
	Region   string // required for constructing default endpoint
	AK       string // SecretID
	SK       string // SecretKey
	Bucket   string // bucket name (with APPID)
}

// requireCOS skips the test if COS credentials are not available.
func requireCOS(t *testing.T) cosEnv {
	t.Helper()
	ak := os.Getenv("NCP_COS_AK")
	sk := os.Getenv("NCP_COS_SK")
	if ak == "" || sk == "" {
		t.Skip("NCP_COS_AK / NCP_COS_SK not set, skipping COS integration test")
	}
	env := cosEnv{
		Endpoint: os.Getenv("NCP_COS_ENDPOINT"),
		Region:   os.Getenv("NCP_COS_REGION"),
		AK:       ak,
		SK:       sk,
		Bucket:   os.Getenv("NCP_COS_BUCKET"),
	}
	if env.Region == "" {
		env.Region = "ap-guangzhou"
	}
	if env.Bucket == "" {
		t.Skip("NCP_COS_BUCKET not set, skipping COS integration test")
	}
	return env
}

// newCOSPrefix returns a unique prefix and registers cleanup.
func newCOSPrefix(t *testing.T, env cosEnv, label string) string {
	t.Helper()
	prefix := fmt.Sprintf("ncp-it/%s/%d/", label, time.Now().UnixNano())
	t.Cleanup(func() { cleanupCOSPrefix(t, env, prefix) })
	return prefix
}

// cleanupCOSPrefix deletes all objects under prefix.
func cleanupCOSPrefix(t *testing.T, env cosEnv, prefix string) {
	t.Helper()
	client := newCOSClient(env)
	ctx := context.Background()

	var marker string
	for {
		result, _, err := client.Bucket.Get(ctx, &cos.BucketGetOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			t.Logf("cleanup list error: %v", err)
			return
		}
		for _, obj := range result.Contents {
			_, _ = client.Object.Delete(ctx, obj.Key)
		}
		if !result.IsTruncated {
			break
		}
		marker = result.NextMarker
	}
}

// newCOSClient creates a raw COS SDK client from env.
func newCOSClient(env cosEnv) *cos.Client {
	baseURLStr := env.Endpoint
	if baseURLStr == "" {
		baseURLStr = fmt.Sprintf("https://%s.cos.%s.myqcloud.com", env.Bucket, env.Region)
	}
	u, _ := url.Parse(baseURLStr)
	base := &cos.BaseURL{BucketURL: u}
	return cos.NewClient(base, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  env.AK,
			SecretKey: env.SK,
		},
	})
}

// newCOSSource creates an ncp cos.Source for testing.
func newCOSSource(t *testing.T, env cosEnv, prefix string) *cosbackend.Source {
	t.Helper()
	src, err := cosbackend.NewSource(cosbackend.SourceConfig{
		Endpoint: env.Endpoint,
		Region:   env.Region,
		AK:       env.AK,
		SK:       env.SK,
		Bucket:   env.Bucket,
		Prefix:   prefix,
	})
	if err != nil {
		t.Fatalf("cos.NewSource: %v", err)
	}
	return src
}

// newCOSDestination creates an ncp cos.Destination for testing.
func newCOSDestination(t *testing.T, env cosEnv, prefix string) *cosbackend.Destination {
	t.Helper()
	dst, err := cosbackend.NewDestination(cosbackend.Config{
		Endpoint: env.Endpoint,
		Region:   env.Region,
		AK:       env.AK,
		SK:       env.SK,
		Bucket:   env.Bucket,
		Prefix:   prefix,
	})
	if err != nil {
		t.Fatalf("cos.NewDestination: %v", err)
	}
	return dst
}

// seedCOSPrefix uploads a map of relative paths to content under the given prefix.
func seedCOSPrefix(t *testing.T, env cosEnv, prefix string, files map[string]string) {
	t.Helper()
	client := newCOSClient(env)
	ctx := context.Background()

	createdDirs := make(map[string]bool)

	for relPath, content := range files {
		key := prefix + relPath

		// Create parent directory markers
		dir := filepath.Dir(relPath)
		for dir != "." && dir != "/" {
			dirKey := prefix + dir + "/"
			if !createdDirs[dirKey] {
				createdDirs[dirKey] = true
				_, err := client.Object.Put(ctx, dirKey, strings.NewReader(""), &cos.ObjectPutOptions{
					ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
						ContentType: "application/x-directory",
						XCosMetaXXX: &http.Header{"ncp-mode": []string{"0755"}},
					},
				})
				if err != nil {
					t.Fatalf("PutObject dir marker %s: %v", dirKey, err)
				}
			}
			dir = filepath.Dir(dir)
		}

		_, err := client.Object.Put(ctx, key, strings.NewReader(content), &cos.ObjectPutOptions{
			ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
				XCosMetaXXX: &http.Header{"ncp-mode": []string{"0644"}},
			},
		})
		if err != nil {
			t.Fatalf("PutObject %s: %v", key, err)
		}
	}
}

// verifyCOSPrefix verifies that objects under prefix match expected content.
func verifyCOSPrefix(t *testing.T, env cosEnv, prefix string, expected map[string]string) {
	t.Helper()
	client := newCOSClient(env)
	ctx := context.Background()
	for relPath, want := range expected {
		key := prefix + relPath
		resp, err := client.Object.Get(ctx, key, nil)
		if err != nil {
			t.Fatalf("GetObject %s: %v", key, err)
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("read body %s: %v", key, err)
		}
		if string(data) != want {
			t.Errorf("content mismatch %s: got %q, want %q", relPath, string(data), want)
		}
	}
}

// putCOSObject overwrites a single object with new content.
func putCOSObject(t *testing.T, env cosEnv, prefix, relPath, content string) {
	t.Helper()
	client := newCOSClient(env)
	key := prefix + relPath
	_, err := client.Object.Put(context.Background(), key, strings.NewReader(content), &cos.ObjectPutOptions{
		ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
			XCosMetaXXX: &http.Header{"ncp-mode": []string{"0644"}},
		},
	})
	if err != nil {
		t.Fatalf("PutObject %s: %v", key, err)
	}
}

// countFilesCOS counts objects under prefix (treating trailing-/ keys as dirs).
func countFilesCOS(t *testing.T, env cosEnv, prefix string) (regulars int, dirs int) {
	t.Helper()
	client := newCOSClient(env)
	ctx := context.Background()

	var marker string
	for {
		result, _, err := client.Bucket.Get(ctx, &cos.BucketGetOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			t.Logf("list error: %v", err)
			return
		}
		for _, obj := range result.Contents {
			key := obj.Key
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
		if !result.IsTruncated {
			break
		}
		marker = result.NextMarker
	}
	return
}
