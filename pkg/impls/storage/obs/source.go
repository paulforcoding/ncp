package obs

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

const (
	metaPrefix = "ncp-"

	metaUID           = metaPrefix + "uid"
	metaGID           = metaPrefix + "gid"
	metaMode          = metaPrefix + "mode"
	metaAtime         = metaPrefix + "atime"
	metaMtime         = metaPrefix + "mtime"
	metaSymlinkTarget = metaPrefix + "symlink-target"
	metaMD5           = metaPrefix + "md5"
	metaXattrPrefix   = metaPrefix + "xattr-"
)

// Source implements storage.Source for Huawei Cloud OBS.
type Source struct {
	client *obs.ObsClient
	bucket string
	prefix string
}

var _ storage.Source = (*Source)(nil)

// SourceConfig holds OBS source configuration.
type SourceConfig struct {
	Endpoint string
	Region   string
	AK       string
	SK       string
	Bucket   string
	Prefix   string
}

// NewSource creates an OBS Source.
func NewSource(cfg SourceConfig) (*Source, error) {
	cli, err := newOBSClient(cfg.AK, cfg.SK, cfg.Endpoint, cfg.Region)
	if err != nil {
		return nil, err
	}
	return &Source{
		client: cli,
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
	}, nil
}

// Walk traverses all objects under prefix using ListObjects with Marker pagination.
// Keys are returned in lexicographic order.
func (s *Source) Walk(ctx context.Context, fn func(context.Context, storage.DiscoverItem) error) error {
	var marker string
	for {
		out, err := s.client.ListObjects(&obs.ListObjectsInput{
			Bucket: s.bucket,
			Marker: marker,
			ListObjsInput: obs.ListObjsInput{
				Prefix:  s.prefix,
				MaxKeys: 1000,
			},
		})
		if err != nil {
			return fmt.Errorf("obs list: %w", err)
		}

		for _, c := range out.Contents {
			relPath := strings.TrimPrefix(c.Key, s.prefix)
			if relPath == "" {
				continue
			}

			item, err := s.objectToItem(c.Key, relPath, c.Size, c.ETag)
			if err != nil {
				continue
			}

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

		if !out.IsTruncated {
			break
		}
		// OBS prefers NextMarker when set; fall back to last key like the SDK examples.
		if out.NextMarker != "" {
			marker = out.NextMarker
		} else if len(out.Contents) > 0 {
			marker = out.Contents[len(out.Contents)-1].Key
		} else {
			break
		}
	}
	return nil
}

// Open opens an OBS object for streaming read.
func (s *Source) Open(ctx context.Context, relPath string) (storage.FileReader, error) {
	key := s.prefix + relPath

	out, err := s.client.GetObject(&obs.GetObjectInput{
		GetObjectMetadataInput: obs.GetObjectMetadataInput{
			Bucket: s.bucket,
			Key:    key,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("obs get %s: %w", relPath, err)
	}

	attr := storage.FileAttr{}
	if out.Metadata != nil {
		attr.Mode = parseMode(out.Metadata[metaMode])
		attr.Uid = parseInt(out.Metadata[metaUID])
		attr.Gid = parseInt(out.Metadata[metaGID])
		if mtime := parseInt64(out.Metadata[metaMtime]); mtime != 0 {
			attr.Mtime = time.Unix(0, mtime)
		}
	}
	if attr.Mode == 0 {
		attr.Mode = 0o644
	}

	return &Reader{
		body: out.Body,
		size: out.ContentLength,
		attr: attr,
	}, nil
}

// Stat rebuilds a DiscoverItem by heading the object.
func (s *Source) Stat(_ context.Context, relPath string) (storage.DiscoverItem, error) {
	key := s.prefix + relPath

	md, err := s.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: s.bucket,
		Key:    key,
	})
	if err != nil {
		dirKey := key + "/"
		md2, err2 := s.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
			Bucket: s.bucket,
			Key:    dirKey,
		})
		if err2 != nil {
			return storage.DiscoverItem{}, fmt.Errorf("obs stat %s: %w", relPath, err)
		}
		md = md2
		key = dirKey
	}

	isDir := strings.HasSuffix(key, "/")
	isSymlink := md.Metadata != nil && md.Metadata[metaSymlinkTarget] != ""

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
		Size:     md.ContentLength,
	}
	item.Checksum, item.Algorithm = parseETag(md.ETag)

	if md.Metadata != nil {
		item.Attr.Mode = parseMode(md.Metadata[metaMode])
		item.Attr.Uid = parseInt(md.Metadata[metaUID])
		item.Attr.Gid = parseInt(md.Metadata[metaGID])
		if mtime := parseInt64(md.Metadata[metaMtime]); mtime != 0 {
			item.Attr.Mtime = time.Unix(0, mtime)
		}
		if ft == model.FileSymlink {
			item.Attr.SymlinkTarget = md.Metadata[metaSymlinkTarget]
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
	return "obs://" + s.bucket + "/" + s.prefix
}

// BeginTask is a no-op for OBS sources.
func (s *Source) BeginTask(ctx context.Context, taskID string) error { return nil }

// EndTask is a no-op for OBS sources.
func (s *Source) EndTask(ctx context.Context, summary storage.TaskSummary) error { return nil }

func (s *Source) objectToItem(key, relPath string, size int64, etag string) (storage.DiscoverItem, error) {
	isDir := strings.HasSuffix(key, "/")

	if isDir {
		item := storage.DiscoverItem{
			RelPath:  strings.TrimSuffix(relPath, "/"),
			FileType: model.FileDir,
			Size:     0,
		}

		md, err := s.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
			Bucket: s.bucket,
			Key:    key,
		})
		if err == nil && md.Metadata != nil {
			item.Attr.Mode = parseMode(md.Metadata[metaMode])
			item.Attr.Uid = parseInt(md.Metadata[metaUID])
			item.Attr.Gid = parseInt(md.Metadata[metaGID])
			if mtime := parseInt64(md.Metadata[metaMtime]); mtime != 0 {
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

	md, err := s.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: s.bucket,
		Key:    key,
	})
	if err == nil && md.Metadata != nil {
		item.Attr.Mode = parseMode(md.Metadata[metaMode])
		item.Attr.Uid = parseInt(md.Metadata[metaUID])
		item.Attr.Gid = parseInt(md.Metadata[metaGID])
		if mtime := parseInt64(md.Metadata[metaMtime]); mtime != 0 {
			item.Attr.Mtime = time.Unix(0, mtime)
		}
		if md.Metadata[metaSymlinkTarget] != "" {
			item.FileType = model.FileSymlink
			item.Attr.SymlinkTarget = md.Metadata[metaSymlinkTarget]
		}
	}
	if item.Attr.Mode == 0 {
		item.Attr.Mode = 0o644
	}

	return item, nil
}

// --- Reader ---

// Reader implements storage.FileReader for OBS objects using a streaming GetObject body.
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

// parseETag converts an OBS ETag string into (checksum []byte, algorithm string).
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

// newOBSClient builds an ObsClient. Endpoint is required; if empty,
// fall back to the standard obs.<region>.myhuaweicloud.com pattern.
func newOBSClient(ak, sk, endpoint, region string) (*obs.ObsClient, error) {
	if endpoint == "" {
		if region == "" {
			return nil, fmt.Errorf("obs: Endpoint or Region is required")
		}
		endpoint = fmt.Sprintf("https://obs.%s.myhuaweicloud.com", region)
	}
	cli, err := obs.New(ak, sk, endpoint)
	if err != nil {
		return nil, fmt.Errorf("obs: new client: %w", err)
	}
	return cli, nil
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
