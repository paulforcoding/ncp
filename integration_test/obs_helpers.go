//go:build integration

package integration

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	obsbackend "github.com/zp001/ncp/pkg/impls/storage/obs"
)

// obsEnv holds OBS credentials and endpoint info.
type obsEnv struct {
	Endpoint string
	Region   string
	AK       string
	SK       string
	Bucket   string
}

// requireOBS skips the test if OBS credentials are not available.
func requireOBS(t *testing.T) obsEnv {
	t.Helper()
	ak := os.Getenv("NCP_OBS_AK")
	sk := os.Getenv("NCP_OBS_SK")
	if ak == "" || sk == "" {
		t.Skip("NCP_OBS_AK / NCP_OBS_SK not set, skipping OBS integration test")
	}
	env := obsEnv{
		Endpoint: os.Getenv("NCP_OBS_ENDPOINT"),
		Region:   os.Getenv("NCP_OBS_REGION"),
		AK:       ak,
		SK:       sk,
		Bucket:   os.Getenv("NCP_OBS_BUCKET"),
	}
	if env.Region == "" {
		env.Region = "cn-north-4"
	}
	if env.Endpoint == "" {
		env.Endpoint = fmt.Sprintf("https://obs.%s.myhuaweicloud.com", env.Region)
	}
	if env.Bucket == "" {
		t.Skip("NCP_OBS_BUCKET not set, skipping OBS integration test")
	}
	return env
}

// newOBSPrefix returns a unique prefix and registers cleanup.
func newOBSPrefix(t *testing.T, env obsEnv, label string) string {
	t.Helper()
	prefix := fmt.Sprintf("ncp-it/%s/%d/", label, time.Now().UnixNano())
	t.Cleanup(func() { cleanupOBSPrefix(t, env, prefix) })
	return prefix
}

// cleanupOBSPrefix deletes all objects under prefix.
func cleanupOBSPrefix(t *testing.T, env obsEnv, prefix string) {
	t.Helper()
	client := newOBSClientHelper(t, env)

	var marker string
	for {
		out, err := client.ListObjects(&obs.ListObjectsInput{
			Bucket: env.Bucket,
			Marker: marker,
			ListObjsInput: obs.ListObjsInput{
				Prefix:  prefix,
				MaxKeys: 1000,
			},
		})
		if err != nil {
			t.Logf("cleanup list error: %v", err)
			return
		}
		for _, c := range out.Contents {
			_, _ = client.DeleteObject(&obs.DeleteObjectInput{
				Bucket: env.Bucket,
				Key:    c.Key,
			})
		}
		if !out.IsTruncated {
			break
		}
		if out.NextMarker != "" {
			marker = out.NextMarker
		} else if len(out.Contents) > 0 {
			marker = out.Contents[len(out.Contents)-1].Key
		} else {
			break
		}
	}
}

// newOBSClientHelper creates a raw OBS SDK client from env.
func newOBSClientHelper(t *testing.T, env obsEnv) *obs.ObsClient {
	t.Helper()
	cli, err := obs.New(env.AK, env.SK, env.Endpoint)
	if err != nil {
		t.Fatalf("obs.New: %v", err)
	}
	return cli
}

// newOBSSource creates an ncp obs.Source for testing.
func newOBSSource(t *testing.T, env obsEnv, prefix string) *obsbackend.Source {
	t.Helper()
	src, err := obsbackend.NewSource(obsbackend.SourceConfig{
		Endpoint: env.Endpoint,
		Region:   env.Region,
		AK:       env.AK,
		SK:       env.SK,
		Bucket:   env.Bucket,
		Prefix:   prefix,
	})
	if err != nil {
		t.Fatalf("obs.NewSource: %v", err)
	}
	return src
}

// newOBSDestination creates an ncp obs.Destination for testing.
func newOBSDestination(t *testing.T, env obsEnv, prefix string) *obsbackend.Destination {
	t.Helper()
	dst, err := obsbackend.NewDestination(obsbackend.Config{
		Endpoint: env.Endpoint,
		Region:   env.Region,
		AK:       env.AK,
		SK:       env.SK,
		Bucket:   env.Bucket,
		Prefix:   prefix,
	})
	if err != nil {
		t.Fatalf("obs.NewDestination: %v", err)
	}
	return dst
}

// newOBSDestinationWithPartSize creates an ncp obs.Destination with custom PartSize.
func newOBSDestinationWithPartSize(t *testing.T, env obsEnv, prefix string, partSize int64) *obsbackend.Destination {
	t.Helper()
	dst, err := obsbackend.NewDestination(obsbackend.Config{
		Endpoint: env.Endpoint,
		Region:   env.Region,
		AK:       env.AK,
		SK:       env.SK,
		Bucket:   env.Bucket,
		Prefix:   prefix,
		PartSize: partSize,
	})
	if err != nil {
		t.Fatalf("obs.NewDestination: %v", err)
	}
	return dst
}

// obsObjectMetadata holds metadata read from a GetObjectMetadata call.
type obsObjectMetadata struct {
	ContentLength int64
	Metadata      map[string]string
}

// headOBSObjectMetadata reads object metadata via GetObjectMetadata.
func headOBSObjectMetadata(t *testing.T, env obsEnv, prefix, relPath string) obsObjectMetadata {
	t.Helper()
	client := newOBSClientHelper(t, env)
	key := prefix + relPath
	out, err := client.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: env.Bucket,
		Key:    key,
	})
	if err != nil {
		t.Fatalf("GetObjectMetadata %s: %v", key, err)
	}
	return obsObjectMetadata{
		ContentLength: out.ContentLength,
		Metadata:      out.Metadata,
	}
}

// seedOBSPrefix uploads a map of relative paths to content under the given prefix.
// Also creates directory marker objects and POSIX metadata for cksum/copy tests.
func seedOBSPrefix(t *testing.T, env obsEnv, prefix string, files map[string]string) {
	t.Helper()
	client := newOBSClientHelper(t, env)

	createdDirs := make(map[string]bool)

	for relPath, content := range files {
		key := prefix + relPath

		dir := filepath.Dir(relPath)
		for dir != "." && dir != "/" {
			dirKey := prefix + dir + "/"
			if !createdDirs[dirKey] {
				createdDirs[dirKey] = true
				_, err := client.PutObject(&obs.PutObjectInput{
					PutObjectBasicInput: obs.PutObjectBasicInput{
						ObjectOperationInput: obs.ObjectOperationInput{
							Bucket:   env.Bucket,
							Key:      dirKey,
							Metadata: map[string]string{"ncp-mode": "0755"},
						},
						HttpHeader: obs.HttpHeader{ContentType: "application/x-directory"},
					},
					Body: strings.NewReader(""),
				})
				if err != nil {
					t.Fatalf("PutObject dir marker %s: %v", dirKey, err)
				}
			}
			dir = filepath.Dir(dir)
		}

		_, err := client.PutObject(&obs.PutObjectInput{
			PutObjectBasicInput: obs.PutObjectBasicInput{
				ObjectOperationInput: obs.ObjectOperationInput{
					Bucket:   env.Bucket,
					Key:      key,
					Metadata: map[string]string{"ncp-mode": "0644"},
				},
			},
			Body: strings.NewReader(content),
		})
		if err != nil {
			t.Fatalf("PutObject %s: %v", key, err)
		}
	}
}

// verifyOBSPrefix verifies that objects under prefix match expected content.
func verifyOBSPrefix(t *testing.T, env obsEnv, prefix string, expected map[string]string) {
	t.Helper()
	client := newOBSClientHelper(t, env)
	for relPath, want := range expected {
		key := prefix + relPath
		out, err := client.GetObject(&obs.GetObjectInput{
			GetObjectMetadataInput: obs.GetObjectMetadataInput{
				Bucket: env.Bucket,
				Key:    key,
			},
		})
		if err != nil {
			t.Fatalf("GetObject %s: %v", key, err)
		}
		data, err := io.ReadAll(out.Body)
		out.Body.Close()
		if err != nil {
			t.Fatalf("read body %s: %v", key, err)
		}
		if string(data) != want {
			t.Errorf("content mismatch %s: got %q, want %q", relPath, string(data), want)
		}
	}
}

// putOBSObject overwrites a single object with new content.
func putOBSObject(t *testing.T, env obsEnv, prefix, relPath, content string) {
	t.Helper()
	client := newOBSClientHelper(t, env)
	key := prefix + relPath
	_, err := client.PutObject(&obs.PutObjectInput{
		PutObjectBasicInput: obs.PutObjectBasicInput{
			ObjectOperationInput: obs.ObjectOperationInput{
				Bucket:   env.Bucket,
				Key:      key,
				Metadata: map[string]string{"ncp-mode": "0644"},
			},
		},
		Body: strings.NewReader(content),
	})
	if err != nil {
		t.Fatalf("PutObject %s: %v", key, err)
	}
}

// uploadOBSMultipartNoNcpMD5 uploads a multipart object via raw SDK without ncp-md5 metadata.
func uploadOBSMultipartNoNcpMD5(t *testing.T, env obsEnv, prefix, relPath string, content []byte, partSize int64) {
	t.Helper()
	client := newOBSClientHelper(t, env)
	key := prefix + relPath

	initOut, err := client.InitiateMultipartUpload(&obs.InitiateMultipartUploadInput{
		ObjectOperationInput: obs.ObjectOperationInput{
			Bucket: env.Bucket,
			Key:    key,
		},
	})
	if err != nil {
		t.Fatalf("InitiateMultipartUpload %s: %v", key, err)
	}
	uploadID := initOut.UploadId

	var parts []obs.Part
	partNum := 0
	for offset := int64(0); offset < int64(len(content)); {
		end := offset + partSize
		if end > int64(len(content)) {
			end = int64(len(content))
		}
		data := content[offset:end]
		partNum++
		partMD5 := md5.Sum(data)

		out, err := client.UploadPart(&obs.UploadPartInput{
			Bucket:     env.Bucket,
			Key:        key,
			UploadId:   uploadID,
			PartNumber: partNum,
			ContentMD5: base64.StdEncoding.EncodeToString(partMD5[:]),
			Body:       bytes.NewReader(data),
			PartSize:   int64(len(data)),
		})
		if err != nil {
			_, _ = client.AbortMultipartUpload(&obs.AbortMultipartUploadInput{
				Bucket:   env.Bucket,
				Key:      key,
				UploadId: uploadID,
			})
			t.Fatalf("UploadPart %d: %v", partNum, err)
		}
		parts = append(parts, obs.Part{
			PartNumber: partNum,
			ETag:       out.ETag,
		})
		offset = end
	}

	_, err = client.CompleteMultipartUpload(&obs.CompleteMultipartUploadInput{
		Bucket:   env.Bucket,
		Key:      key,
		UploadId: uploadID,
		Parts:    parts,
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload %s: %v", key, err)
	}
}

// countFilesOBS counts objects under prefix (treating trailing-/ keys as dirs).
func countFilesOBS(t *testing.T, env obsEnv, prefix string) (regulars int, dirs int) {
	t.Helper()
	client := newOBSClientHelper(t, env)

	var marker string
	for {
		out, err := client.ListObjects(&obs.ListObjectsInput{
			Bucket: env.Bucket,
			Marker: marker,
			ListObjsInput: obs.ListObjsInput{
				Prefix:  prefix,
				MaxKeys: 1000,
			},
		})
		if err != nil {
			t.Logf("list error: %v", err)
			return
		}
		for _, c := range out.Contents {
			relPath := strings.TrimPrefix(c.Key, prefix)
			if relPath == "" {
				continue
			}
			if strings.HasSuffix(c.Key, "/") {
				dirs++
			} else {
				regulars++
			}
		}
		if !out.IsTruncated {
			break
		}
		if out.NextMarker != "" {
			marker = out.NextMarker
		} else if len(out.Contents) > 0 {
			marker = out.Contents[len(out.Contents)-1].Key
		} else {
			break
		}
	}
	return
}
