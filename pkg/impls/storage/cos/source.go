package cos

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

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
func (s *Source) Walk(ctx context.Context, fn func(model.DiscoverItem) error) error {
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

			if err := fn(item); err != nil {
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

// Open opens a COS object for reading with Range support.
func (s *Source) Open(relPath string) (storage.Reader, error) {
	key := s.prefix + relPath

	resp, err := s.client.Object.Head(context.Background(), key, nil)
	if err != nil {
		return nil, fmt.Errorf("cos head %s: %w", relPath, err)
	}

	size, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	metadata := extractMetadata(resp.Header)

	return &Reader{
		client:   s.client,
		key:      key,
		size:     size,
		metadata: metadata,
	}, nil
}

// Restat rebuilds a DiscoverItem by heading the object.
func (s *Source) Restat(relPath string) (model.DiscoverItem, error) {
	key := s.prefix + relPath

	resp, err := s.client.Object.Head(context.Background(), key, nil)
	if err != nil {
		// Directory marker objects have a trailing "/" in the key.
		dirKey := key + "/"
		resp, err = s.client.Object.Head(context.Background(), dirKey, nil)
		if err != nil {
			return model.DiscoverItem{}, fmt.Errorf("cos restat %s: %w", relPath, err)
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
	etag := strings.ToLower(strings.Trim(resp.Header.Get("ETag"), `"`))

	item := model.DiscoverItem{
		SrcBase:  "cos://" + s.bucket + "/" + s.prefix,
		RelPath:  relPath,
		FileType: ft,
		FileSize: size,
		ETag:     etag,
	}

	item.Mode = parseMode(metadata[metaMode])
	item.Uid = parseInt(metadata[metaUID])
	item.Gid = parseInt(metadata[metaGID])
	if ft == model.FileSymlink {
		item.LinkTarget = metadata[metaSymlinkTarget]
	}
	item.Mtime = parseInt64(metadata[metaMtime])

	if item.Mode == 0 {
		if isDir {
			item.Mode = 0o755
		} else {
			item.Mode = 0o644
		}
	}

	return item, nil
}

func (s *Source) Base() string {
	return "cos://" + s.bucket + "/" + s.prefix
}

func (s *Source) objectToItem(key, relPath string, size int64, etag string) (model.DiscoverItem, error) {
	isDir := strings.HasSuffix(key, "/")

	if isDir {
		item := model.DiscoverItem{
			SrcBase:  "cos://" + s.bucket + "/" + s.prefix,
			RelPath:  strings.TrimSuffix(relPath, "/"),
			FileType: model.FileDir,
			FileSize: 0,
		}

		resp, err := s.client.Object.Head(context.Background(), key, nil)
		if err == nil {
			metadata := extractMetadata(resp.Header)
			item.Mode = parseMode(metadata[metaMode])
			item.Uid = parseInt(metadata[metaUID])
			item.Gid = parseInt(metadata[metaGID])
			item.Mtime = parseInt64(metadata[metaMtime])
		}
		if item.Mode == 0 {
			item.Mode = 0o755
		}
		return item, nil
	}

	item := model.DiscoverItem{
		SrcBase:  "cos://" + s.bucket + "/" + s.prefix,
		RelPath:  relPath,
		FileType: model.FileRegular,
		FileSize: size,
		ETag:     strings.ToLower(strings.Trim(etag, `"`)),
	}

	resp, err := s.client.Object.Head(context.Background(), key, nil)
	if err == nil {
		metadata := extractMetadata(resp.Header)
		item.Mode = parseMode(metadata[metaMode])
		item.Uid = parseInt(metadata[metaUID])
		item.Gid = parseInt(metadata[metaGID])
		item.Mtime = parseInt64(metadata[metaMtime])
		if metadata[metaSymlinkTarget] != "" {
			item.FileType = model.FileSymlink
			item.LinkTarget = metadata[metaSymlinkTarget]
		}
	}
	if item.Mode == 0 {
		item.Mode = 0o644
	}

	return item, nil
}

// --- Reader ---

// Reader implements storage.Reader for COS objects using HTTP Range requests.
type Reader struct {
	client   *cos.Client
	key      string
	size     int64
	metadata map[string]string
}

var _ storage.Reader = (*Reader)(nil)

func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}

	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}

	rangeHeader := fmt.Sprintf("bytes=%d-%d", off, end)

	resp, err := r.client.Object.Get(context.Background(), r.key, &cos.ObjectGetOptions{
		Range: rangeHeader,
	})
	if err != nil {
		return 0, fmt.Errorf("cos get range %s [%d-%d]: %w", r.key, off, end, err)
	}
	defer resp.Body.Close()

	n, err := io.ReadFull(resp.Body, p)
	if err == io.ErrUnexpectedEOF {
		return n, nil
	}
	if err == io.EOF && n == 0 {
		return 0, io.EOF
	}
	return n, err
}

func (r *Reader) Close() error { return nil }

// Metadata returns the object's custom metadata headers.
func (r *Reader) Metadata() map[string]string { return r.metadata }

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
