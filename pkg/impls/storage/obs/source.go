package obs

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

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
func (s *Source) Walk(ctx context.Context, fn func(model.DiscoverItem) error) error {
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

			if err := fn(item); err != nil {
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

// Open opens an OBS object for reading with Range support (ReadAt semantics).
func (s *Source) Open(relPath string) (storage.Reader, error) {
	key := s.prefix + relPath

	md, err := s.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: s.bucket,
		Key:    key,
	})
	if err != nil {
		return nil, fmt.Errorf("obs head %s: %w", relPath, err)
	}

	return &Reader{
		client:   s.client,
		bucket:   s.bucket,
		key:      key,
		size:     md.ContentLength,
		metadata: md.Metadata,
	}, nil
}

// Restat rebuilds a DiscoverItem by heading the object.
func (s *Source) Restat(relPath string) (model.DiscoverItem, error) {
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
			return model.DiscoverItem{}, fmt.Errorf("obs restat %s: %w", relPath, err)
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

	item := model.DiscoverItem{
		SrcBase:  s.Base(),
		RelPath:  relPath,
		FileType: ft,
		FileSize: md.ContentLength,
		ETag:     strings.ToLower(strings.Trim(md.ETag, `"`)),
	}

	if md.Metadata != nil {
		item.Mode = parseMode(md.Metadata[metaMode])
		item.Uid = parseInt(md.Metadata[metaUID])
		item.Gid = parseInt(md.Metadata[metaGID])
		item.Mtime = parseInt64(md.Metadata[metaMtime])
		if ft == model.FileSymlink {
			item.LinkTarget = md.Metadata[metaSymlinkTarget]
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
	return "obs://" + s.bucket + "/" + s.prefix
}

func (s *Source) objectToItem(key, relPath string, size int64, etag string) (model.DiscoverItem, error) {
	isDir := strings.HasSuffix(key, "/")

	if isDir {
		item := model.DiscoverItem{
			SrcBase:  s.Base(),
			RelPath:  strings.TrimSuffix(relPath, "/"),
			FileType: model.FileDir,
			FileSize: 0,
		}

		md, err := s.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
			Bucket: s.bucket,
			Key:    key,
		})
		if err == nil && md.Metadata != nil {
			item.Mode = parseMode(md.Metadata[metaMode])
			item.Uid = parseInt(md.Metadata[metaUID])
			item.Gid = parseInt(md.Metadata[metaGID])
			item.Mtime = parseInt64(md.Metadata[metaMtime])
		}
		if item.Mode == 0 {
			item.Mode = 0o755
		}
		return item, nil
	}

	item := model.DiscoverItem{
		SrcBase:  s.Base(),
		RelPath:  relPath,
		FileType: model.FileRegular,
		FileSize: size,
		ETag:     strings.ToLower(strings.Trim(etag, `"`)),
	}

	md, err := s.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: s.bucket,
		Key:    key,
	})
	if err == nil && md.Metadata != nil {
		item.Mode = parseMode(md.Metadata[metaMode])
		item.Uid = parseInt(md.Metadata[metaUID])
		item.Gid = parseInt(md.Metadata[metaGID])
		item.Mtime = parseInt64(md.Metadata[metaMtime])
		if md.Metadata[metaSymlinkTarget] != "" {
			item.FileType = model.FileSymlink
			item.LinkTarget = md.Metadata[metaSymlinkTarget]
		}
	}
	if item.Mode == 0 {
		item.Mode = 0o644
	}

	return item, nil
}

// --- Reader ---

// Reader implements storage.Reader for OBS objects using HTTP Range requests.
type Reader struct {
	client   *obs.ObsClient
	bucket   string
	key      string
	size     int64
	metadata map[string]string
}

var _ storage.Reader = (*Reader)(nil)

// ReadAt reads len(p) bytes from the object starting at byte offset off.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}

	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}

	out, err := r.client.GetObject(&obs.GetObjectInput{
		GetObjectMetadataInput: obs.GetObjectMetadataInput{
			Bucket: r.bucket,
			Key:    r.key,
		},
		RangeStart: off,
		RangeEnd:   end,
	})
	if err != nil {
		return 0, fmt.Errorf("obs get range %s [%d-%d]: %w", r.key, off, end, err)
	}
	defer out.Body.Close()

	n, err := io.ReadFull(out.Body, p)
	if err == io.ErrUnexpectedEOF {
		return n, nil
	}
	if err == io.EOF && n == 0 {
		return 0, io.EOF
	}
	return n, err
}

// Close is a no-op for OBS Reader.
func (r *Reader) Close() error { return nil }

// Metadata returns the object's custom metadata headers.
func (r *Reader) Metadata() map[string]string { return r.metadata }

// --- Helpers ---

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
