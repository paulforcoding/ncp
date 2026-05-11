package aliyun

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// Source implements storage.Source for Alibaba Cloud OSS.
type Source struct {
	client *oss.Client
	bucket string
	prefix string // key prefix (e.g. "backup/")
}

var _ storage.Source = (*Source)(nil)

// SourceConfig holds OSS source configuration.
type SourceConfig struct {
	Endpoint string
	Region   string
	AK       string
	SK       string
	Bucket   string
	Prefix   string
}

// NewSource creates an OSS Source.
func NewSource(cfg SourceConfig) (*Source, error) {
	ossCfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AK, cfg.SK)).
		WithRegion(cfg.Region).
		WithEndpoint(cfg.Endpoint)
	client := oss.NewClient(ossCfg)
	return &Source{
		client: client,
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
	}, nil
}

// Walk traverses all objects under prefix using ListObjectsV2 pagination.
// Keys are returned in lexicographic order (DFS property preserved).
func (s *Source) Walk(ctx context.Context, fn func(context.Context, storage.DiscoverItem) error) error {
	var continuationToken *string

	for {
		result, err := s.client.ListObjectsV2(ctx, &oss.ListObjectsV2Request{
			Bucket:            oss.Ptr(s.bucket),
			Prefix:            oss.Ptr(s.prefix),
			ContinuationToken: continuationToken,
			MaxKeys:           1000,
		})
		if err != nil {
			return fmt.Errorf("aliyun list: %w", err)
		}

		for _, obj := range result.Contents {
			key := oss.ToString(obj.Key)

			// Strip prefix to get relative path
			relPath := strings.TrimPrefix(key, s.prefix)
			if relPath == "" {
				continue
			}

			item, err := s.objectToItem(key, relPath, obj.Size, oss.ToString(obj.ETag))
			if err != nil {
				continue
			}

			// Skip root directory marker
			if relPath == "/" || (item.FileType == model.FileDir && relPath == "") {
				continue
			}

			if err := fn(ctx, item); err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}

		if !result.IsTruncated {
			break
		}
		continuationToken = result.NextContinuationToken
	}

	return nil
}

// Open opens an OSS object for streaming read.
func (s *Source) Open(ctx context.Context, relPath string) (storage.FileReader, error) {
	key := s.prefix + relPath

	result, err := s.client.GetObject(ctx, &oss.GetObjectRequest{
		Bucket: oss.Ptr(s.bucket),
		Key:    oss.Ptr(key),
	})
	if err != nil {
		return nil, fmt.Errorf("aliyun get %s: %w", relPath, err)
	}

	attr := storage.FileAttr{}
	if result.Metadata != nil {
		attr.Mode = parseMode(result.Metadata[metaMode])
		attr.Uid = parseInt(result.Metadata[metaUID])
		attr.Gid = parseInt(result.Metadata[metaGID])
		if mtime := parseInt64(result.Metadata[metaMtime]); mtime != 0 {
			attr.Mtime = time.Unix(0, mtime)
		}
	}
	if attr.Mode == 0 {
		attr.Mode = 0o644
	}

	return &Reader{
		body: result.Body,
		size: result.ContentLength,
		attr: attr,
	}, nil
}

// Stat rebuilds a DiscoverItem by heading the object.
func (s *Source) Stat(ctx context.Context, relPath string) (storage.DiscoverItem, error) {
	key := s.prefix + relPath

	result, err := s.client.HeadObject(ctx, &oss.HeadObjectRequest{
		Bucket: oss.Ptr(s.bucket),
		Key:    oss.Ptr(key),
	})
	if err != nil {
		// Directory marker objects in OSS have a trailing "/" in the key.
		// If the direct lookup fails, try with "/" appended.
		dirKey := key + "/"
		result, err = s.client.HeadObject(ctx, &oss.HeadObjectRequest{
			Bucket: oss.Ptr(s.bucket),
			Key:    oss.Ptr(dirKey),
		})
		if err != nil {
			return storage.DiscoverItem{}, fmt.Errorf("aliyun stat %s: %w", relPath, err)
		}
		key = dirKey
	}

	isDir := strings.HasSuffix(key, "/")
	isSymlink := result.Metadata != nil && result.Metadata[metaSymlinkTarget] != ""

	var ft model.FileType
	switch {
	case isDir:
		ft = model.FileDir
	case isSymlink:
		ft = model.FileSymlink
	default:
		ft = model.FileRegular
	}

	item := storage.DiscoverItem{
		RelPath:  relPath,
		FileType: ft,
		Size:     result.ContentLength,
	}

	if result.ETag != nil {
		item.Checksum, item.Algorithm = parseETag(*result.ETag)
	}

	if result.Metadata != nil {
		item.Attr.Mode = parseMode(result.Metadata[metaMode])
		item.Attr.Uid = parseInt(result.Metadata[metaUID])
		item.Attr.Gid = parseInt(result.Metadata[metaGID])
		if mtime := parseInt64(result.Metadata[metaMtime]); mtime != 0 {
			item.Attr.Mtime = time.Unix(0, mtime)
		}
		if ft == model.FileSymlink {
			item.Attr.SymlinkTarget = result.Metadata[metaSymlinkTarget]
		}
	}
	if item.Attr.Mode == 0 {
		if isDir {
			item.Attr.Mode = 0o755
		} else {
			item.Attr.Mode = 0o644
		}
	}

	return item, nil
}

// Base returns the source base URI.
func (s *Source) URI() string {
	return "oss://" + s.bucket + "/" + s.prefix
}

// BeginTask is a no-op for OSS sources.
func (s *Source) BeginTask(ctx context.Context, taskID string) error { return nil }

// EndTask is a no-op for OSS sources.
func (s *Source) EndTask(ctx context.Context, summary storage.TaskSummary) error { return nil }

func (s *Source) objectToItem(key, relPath string, size int64, etag string) (storage.DiscoverItem, error) {
	isDir := strings.HasSuffix(key, "/")

	if isDir {
		item := storage.DiscoverItem{
			RelPath:  strings.TrimSuffix(relPath, "/"),
			FileType: model.FileDir,
			Size:     0,
		}

		result, err := s.client.HeadObject(context.Background(), &oss.HeadObjectRequest{
			Bucket: oss.Ptr(s.bucket),
			Key:    oss.Ptr(key),
		})
		if err == nil && result.Metadata != nil {
			item.Attr.Mode = parseMode(result.Metadata[metaMode])
			item.Attr.Uid = parseInt(result.Metadata[metaUID])
			item.Attr.Gid = parseInt(result.Metadata[metaGID])
			if mtime := parseInt64(result.Metadata[metaMtime]); mtime != 0 {
				item.Attr.Mtime = time.Unix(0, mtime)
			}
		}
		if item.Attr.Mode == 0 {
			item.Attr.Mode = 0o755
		}

		return item, nil
	}

	item := storage.DiscoverItem{
		RelPath:  relPath,
		FileType: model.FileRegular,
		Size:     size,
	}
	item.Checksum, item.Algorithm = parseETag(etag)

	result, err := s.client.HeadObject(context.Background(), &oss.HeadObjectRequest{
		Bucket: oss.Ptr(s.bucket),
		Key:    oss.Ptr(key),
	})
	if err == nil && result.Metadata != nil {
		item.Attr.Mode = parseMode(result.Metadata[metaMode])
		item.Attr.Uid = parseInt(result.Metadata[metaUID])
		item.Attr.Gid = parseInt(result.Metadata[metaGID])
		if mtime := parseInt64(result.Metadata[metaMtime]); mtime != 0 {
			item.Attr.Mtime = time.Unix(0, mtime)
		}
		if result.Metadata[metaSymlinkTarget] != "" {
			item.FileType = model.FileSymlink
			item.Attr.SymlinkTarget = result.Metadata[metaSymlinkTarget]
		}
	}
	if item.Attr.Mode == 0 {
		item.Attr.Mode = 0o644
	}

	return item, nil
}

// --- Reader ---

// Reader implements storage.FileReader for OSS objects using a streaming GetObject body.
type Reader struct {
	body io.ReadCloser
	size int64
	attr storage.FileAttr
}

var _ storage.FileReader = (*Reader)(nil)

// Read reads up to len(p) bytes from the object body.
func (r *Reader) Read(ctx context.Context, p []byte) (int, error) {
	return r.body.Read(p)
}

// Close closes the underlying object body.
func (r *Reader) Close(ctx context.Context) error {
	return r.body.Close()
}

// Size returns the object content length.
func (r *Reader) Size() int64 { return r.size }

// Attr returns the object metadata.
func (r *Reader) Attr() storage.FileAttr { return r.attr }

// --- Helpers ---

// parseETag converts an OSS ETag string into (checksum []byte, algorithm string).
// Single-part objects have an ETag that is a lowercase hex MD5 (decoded to bytes,
// algorithm "etag-md5"). Multipart objects have ETags of the form "<md5-of-md5s>-<N>"
// which cannot be decoded to bytes; the literal string is stored as UTF-8 bytes
// with algorithm "etag-multipart".
func parseETag(etag string) ([]byte, string) {
	etag = strings.ToLower(strings.Trim(etag, `"`))
	if etag == "" {
		return nil, ""
	}
	if strings.Contains(etag, "-") {
		return []byte(etag), "etag-multipart"
	}
	if b, err := hex.DecodeString(etag); err == nil {
		return b, "etag-md5"
	}
	return []byte(etag), "etag-multipart"
}

func parseMode(s string) os.FileMode {
	if s == "" {
		return 0
	}
	m, err := strconv.ParseUint(s, 8, 32)
	if err != nil {
		return 0
	}
	return os.FileMode(m)
}

func parseInt(s string) int {
	if s == "" {
		return 0
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return v
}

func parseInt64(s string) int64 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}
