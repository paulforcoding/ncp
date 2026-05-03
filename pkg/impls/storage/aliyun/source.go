package aliyun

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

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
func (s *Source) Walk(ctx context.Context, fn func(model.DiscoverItem) error) error {
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

			if err := fn(item); err != nil {
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

// Open opens an OSS object for reading with Range support (ReadAt semantics).
func (s *Source) Open(relPath string) (storage.Reader, error) {
	key := s.prefix + relPath

	// HeadObject to get content length for ReadAt range construction
	result, err := s.client.HeadObject(context.Background(), &oss.HeadObjectRequest{
		Bucket: oss.Ptr(s.bucket),
		Key:    oss.Ptr(key),
	})
	if err != nil {
		return nil, fmt.Errorf("aliyun head %s: %w", relPath, err)
	}

	return &Reader{
		client:  s.client,
		bucket:  s.bucket,
		key:     key,
		size:    result.ContentLength,
		metadata: result.Metadata,
	}, nil
}

// Restat rebuilds a DiscoverItem by heading the object.
func (s *Source) Restat(relPath string) (model.DiscoverItem, error) {
	key := s.prefix + relPath

	result, err := s.client.HeadObject(context.Background(), &oss.HeadObjectRequest{
		Bucket: oss.Ptr(s.bucket),
		Key:    oss.Ptr(key),
	})
	if err != nil {
		// Directory marker objects in OSS have a trailing "/" in the key.
		// If the direct lookup fails, try with "/" appended.
		dirKey := key + "/"
		result, err = s.client.HeadObject(context.Background(), &oss.HeadObjectRequest{
			Bucket: oss.Ptr(s.bucket),
			Key:    oss.Ptr(dirKey),
		})
		if err != nil {
			return model.DiscoverItem{}, fmt.Errorf("aliyun restat %s: %w", relPath, err)
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

	item := model.DiscoverItem{
		SrcBase:  "oss://" + s.bucket + "/" + s.prefix,
		RelPath:  relPath,
		FileType: ft,
		FileSize: result.ContentLength,
	}

	if result.ETag != nil {
		item.ETag = strings.ToLower(strings.Trim(*result.ETag, `"`))
	}

	if result.Metadata != nil {
		item.Mode = parseMode(result.Metadata[metaMode])
		item.Uid = parseInt(result.Metadata[metaUID])
		item.Gid = parseInt(result.Metadata[metaGID])
		if ft == model.FileSymlink {
			item.LinkTarget = result.Metadata[metaSymlinkTarget]
		}
	}
	if item.Mode == 0 {
		if isDir {
			item.Mode = 0o755
		} else {
			item.Mode = 0o644
		}
	}

	return item, nil
}

// Base returns the source base URI.
func (s *Source) Base() string {
	return "oss://" + s.bucket + "/" + s.prefix
}

func (s *Source) objectToItem(key, relPath string, size int64, etag string) (model.DiscoverItem, error) {
	isDir := strings.HasSuffix(key, "/")

	if isDir {
		item := model.DiscoverItem{
			SrcBase:  "oss://" + s.bucket + "/" + s.prefix,
			RelPath:  strings.TrimSuffix(relPath, "/"),
			FileType: model.FileDir,
			FileSize: 0,
		}

		result, err := s.client.HeadObject(context.Background(), &oss.HeadObjectRequest{
			Bucket: oss.Ptr(s.bucket),
			Key:    oss.Ptr(key),
		})
		if err == nil && result.Metadata != nil {
			item.Mode = parseMode(result.Metadata[metaMode])
			item.Uid = parseInt(result.Metadata[metaUID])
			item.Gid = parseInt(result.Metadata[metaGID])
			item.Mtime = parseInt64(result.Metadata[metaMtime])
		}
		if item.Mode == 0 {
			item.Mode = 0o755
		}

		return item, nil
	}

	item := model.DiscoverItem{
		SrcBase:  "oss://" + s.bucket + "/" + s.prefix,
		RelPath:  relPath,
		FileType: model.FileRegular,
		FileSize: size,
		ETag:     strings.ToLower(strings.Trim(etag, `"`)),
	}

	result, err := s.client.HeadObject(context.Background(), &oss.HeadObjectRequest{
		Bucket: oss.Ptr(s.bucket),
		Key:    oss.Ptr(key),
	})
	if err == nil && result.Metadata != nil {
		item.Mode = parseMode(result.Metadata[metaMode])
		item.Uid = parseInt(result.Metadata[metaUID])
		item.Gid = parseInt(result.Metadata[metaGID])
		item.Mtime = parseInt64(result.Metadata[metaMtime])
		if result.Metadata[metaSymlinkTarget] != "" {
			item.FileType = model.FileSymlink
			item.LinkTarget = result.Metadata[metaSymlinkTarget]
		}
	}
	if item.Mode == 0 {
		item.Mode = 0o644
	}

	return item, nil
}

// --- Reader ---

// Reader implements storage.Reader for OSS objects using HTTP Range requests.
type Reader struct {
	client   *oss.Client
	bucket   string
	key      string
	size     int64
	metadata map[string]string
}

var _ storage.Reader = (*Reader)(nil)

// ReadAt reads len(p) bytes from the object starting at byte offset off.
// Uses HTTP Range header for partial downloads.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}

	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}

	rangeHeader := fmt.Sprintf("bytes=%d-%d", off, end)

	result, err := r.client.GetObject(context.Background(), &oss.GetObjectRequest{
		Bucket: oss.Ptr(r.bucket),
		Key:    oss.Ptr(r.key),
		Range:  oss.Ptr(rangeHeader),
	})
	if err != nil {
		return 0, fmt.Errorf("aliyun get range %s [%d-%d]: %w", r.key, off, end, err)
	}
	defer result.Body.Close()

	n, err := io.ReadFull(result.Body, p)
	if err == io.ErrUnexpectedEOF {
		return n, nil
	}
	if err == io.EOF && n == 0 {
		return 0, io.EOF
	}
	return n, err
}

// Close is a no-op for OSS Reader (no persistent connection to close).
func (r *Reader) Close() error { return nil }

// Metadata returns the object's custom metadata headers.
func (r *Reader) Metadata() map[string]string { return r.metadata }

// --- Helpers ---

func parseMode(s string) uint32 {
	if s == "" {
		return 0
	}
	m, err := strconv.ParseUint(s, 8, 32)
	if err != nil {
		return 0
	}
	return uint32(m)
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

