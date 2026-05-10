package cos

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tencentyun/cos-go-sdk-v5"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// Source implements storage.Source for Tencent Cloud COS.
type Source struct {
	client *cos.Client
	bucket string
	prefix string
}

var _ storage.Source = (*Source)(nil)

// SourceConfig holds COS source configuration.
type SourceConfig struct {
	Endpoint string // optional custom endpoint; if empty, construct from Region+Bucket
	Region   string // required for constructing default endpoint
	AK       string // SecretID
	SK       string // SecretKey
	Bucket   string // bucket name (with APPID if applicable)
	Prefix   string // key prefix
}

// NewSource creates a COS Source.
func NewSource(cfg SourceConfig) (*Source, error) {
	baseURLStr := cfg.Endpoint
	if baseURLStr == "" {
		if cfg.Region == "" {
			return nil, fmt.Errorf("cos: Region is required when Endpoint is not set")
		}
		baseURLStr = fmt.Sprintf("https://%s.cos.%s.myqcloud.com", cfg.Bucket, cfg.Region)
	}
	u, err := url.Parse(baseURLStr)
	if err != nil {
		return nil, fmt.Errorf("cos: invalid base URL %q: %w", baseURLStr, err)
	}

	base := &cos.BaseURL{BucketURL: u}
	client := cos.NewClient(base, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  cfg.AK,
			SecretKey: cfg.SK,
		},
	})

	return &Source{
		client: client,
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
	}, nil
}

// Walk traverses all objects under prefix using Bucket.Get pagination.
func (s *Source) Walk(ctx context.Context, fn func(context.Context, storage.DiscoverItem) error) error {
	var marker string
	for {
		result, _, err := s.client.Bucket.Get(ctx, &cos.BucketGetOptions{
			Prefix:  s.prefix,
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			return fmt.Errorf("cos list: %w", err)
		}

		for _, obj := range result.Contents {
			key := obj.Key
			relPath := strings.TrimPrefix(key, s.prefix)
			if relPath == "" {
				continue
			}

			item, err := s.objectToItem(key, relPath, obj.Size, obj.ETag)
			if err != nil {
				continue
			}

			if relPath == "/" || (item.FileType == model.FileDir && relPath == "") {
				continue
			}

			if err := fn(ctx, item); err != nil {
				return err
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !result.IsTruncated {
			break
		}
		marker = result.NextMarker
	}
	return nil
}

// Open opens a COS object for streaming read.
func (s *Source) Open(ctx context.Context, relPath string) (storage.FileReader, error) {
	key := s.prefix + relPath

	resp, err := s.client.Object.Get(ctx, key, nil)
	if err != nil {
		return nil, fmt.Errorf("cos get %s: %w", relPath, err)
	}

	size, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	metadata := extractMetadata(resp.Header)

	attr := storage.FileAttr{}
	attr.Mode = parseMode(metadata[metaMode])
	attr.Uid = parseInt(metadata[metaUID])
	attr.Gid = parseInt(metadata[metaGID])
	if mtime := parseInt64(metadata[metaMtime]); mtime != 0 {
		attr.Mtime = time.Unix(0, mtime)
	}
	if attr.Mode == 0 {
		attr.Mode = 0o644
	}

	return &Reader{
		body: resp.Body,
		size: size,
		attr: attr,
	}, nil
}

// Stat rebuilds a DiscoverItem by heading the object.
func (s *Source) Stat(ctx context.Context, relPath string) (storage.DiscoverItem, error) {
	key := s.prefix + relPath

	resp, err := s.client.Object.Head(ctx, key, nil)
	if err != nil {
		// Directory marker objects have a trailing "/" in the key.
		dirKey := key + "/"
		resp, err = s.client.Object.Head(ctx, dirKey, nil)
		if err != nil {
			return storage.DiscoverItem{}, fmt.Errorf("cos stat %s: %w", relPath, err)
		}
		key = dirKey
	}

	metadata := extractMetadata(resp.Header)
	isDir := strings.HasSuffix(key, "/")
	isSymlink := metadata[metaSymlinkTarget] != ""

	var ft model.FileType
	switch {
	case isDir:
		ft = model.FileDir
	case isSymlink:
		ft = model.FileSymlink
	default:
		ft = model.FileRegular
	}

	size, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)

	item := storage.DiscoverItem{
		RelPath:  relPath,
		FileType: ft,
		Size:     size,
	}
	item.Checksum, item.Algorithm = parseETag(resp.Header.Get("ETag"))

	item.Attr.Mode = parseMode(metadata[metaMode])
	item.Attr.Uid = parseInt(metadata[metaUID])
	item.Attr.Gid = parseInt(metadata[metaGID])
	if ft == model.FileSymlink {
		item.Attr.SymlinkTarget = metadata[metaSymlinkTarget]
	}
	if mtime := parseInt64(metadata[metaMtime]); mtime != 0 {
		item.Attr.Mtime = time.Unix(0, mtime)
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

func (s *Source) URI() string {
	return "cos://" + s.bucket + "/" + s.prefix
}

// Connect is a no-op for COS sources (client is initialized in constructor).
func (s *Source) Connect(ctx context.Context) error { return nil }

// Close is a no-op for COS sources.
func (s *Source) Close(ctx context.Context) error { return nil }

// BeginTask is a no-op for COS sources.
func (s *Source) BeginTask(ctx context.Context, taskID string) error { return nil }

// EndTask is a no-op for COS sources.
func (s *Source) EndTask(ctx context.Context, summary storage.TaskSummary) error { return nil }

func (s *Source) objectToItem(key, relPath string, size int64, etag string) (storage.DiscoverItem, error) {
	isDir := strings.HasSuffix(key, "/")

	if isDir {
		item := storage.DiscoverItem{
			RelPath:  strings.TrimSuffix(relPath, "/"),
			FileType: model.FileDir,
			Size:     0,
		}

		resp, err := s.client.Object.Head(context.Background(), key, nil)
		if err == nil {
			metadata := extractMetadata(resp.Header)
			item.Attr.Mode = parseMode(metadata[metaMode])
			item.Attr.Uid = parseInt(metadata[metaUID])
			item.Attr.Gid = parseInt(metadata[metaGID])
			if mtime := parseInt64(metadata[metaMtime]); mtime != 0 {
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

	resp, err := s.client.Object.Head(context.Background(), key, nil)
	if err == nil {
		metadata := extractMetadata(resp.Header)
		item.Attr.Mode = parseMode(metadata[metaMode])
		item.Attr.Uid = parseInt(metadata[metaUID])
		item.Attr.Gid = parseInt(metadata[metaGID])
		if mtime := parseInt64(metadata[metaMtime]); mtime != 0 {
			item.Attr.Mtime = time.Unix(0, mtime)
		}
		if metadata[metaSymlinkTarget] != "" {
			item.FileType = model.FileSymlink
			item.Attr.SymlinkTarget = metadata[metaSymlinkTarget]
		}
	}
	if item.Attr.Mode == 0 {
		item.Attr.Mode = 0o644
	}

	return item, nil
}

// --- Reader ---

// Reader implements storage.FileReader for COS objects using a streaming GetObject body.
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

func extractMetadata(h http.Header) map[string]string {
	meta := make(map[string]string)
	for k, v := range h {
		lowerKey := strings.ToLower(k)
		if strings.HasPrefix(lowerKey, "x-cos-meta-") && len(v) > 0 {
			metaKey := strings.TrimPrefix(lowerKey, "x-cos-meta-")
			meta[metaKey] = v[0]
		}
	}
	return meta
}

// parseETag converts a COS ETag string into (checksum []byte, algorithm string).
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
