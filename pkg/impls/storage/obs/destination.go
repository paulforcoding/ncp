package obs

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
	"os"
	"strings"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

const (
	minPartSize        = 5 << 20 // 5MB — align with OSS for cross-cloud parity
	smallFileThreshold = 5 << 20 // below this, use single PutObject
)

// Destination implements storage.Destination for Huawei Cloud OBS.
type Destination struct {
	client   *obs.ObsClient
	bucket   string
	prefix   string
	retryCfg RetryConfig
}

var _ storage.Destination = (*Destination)(nil)
var _ storage.Restatter = (*Destination)(nil)

// Config holds OBS destination configuration.
type Config struct {
	Endpoint string
	Region   string
	AK       string
	SK       string
	Bucket   string
	Prefix   string
	RetryCfg RetryConfig
}

// NewDestination creates an OBS Destination.
func NewDestination(cfg Config) (*Destination, error) {
	cli, err := newOBSClient(cfg.AK, cfg.SK, cfg.Endpoint, cfg.Region)
	if err != nil {
		return nil, err
	}
	rc := cfg.RetryCfg
	if rc.MaxAttempts == 0 {
		rc = DefaultRetryConfig()
	}
	return &Destination{
		client:   cli,
		bucket:   cfg.Bucket,
		prefix:   cfg.Prefix,
		retryCfg: rc,
	}, nil
}

func (d *Destination) key(relPath string) string {
	return d.prefix + relPath
}

// Mkdir creates a zero-byte marker object with key ending in "/".
func (d *Destination) Mkdir(ctx context.Context, relPath string, mode os.FileMode, uid, gid int) error {
	key := d.key(relPath + "/")
	meta := posixMetadata(mode, uid, gid)
	err := withRetry(ctx, d.retryCfg, func() error {
		_, err := d.client.PutObject(&obs.PutObjectInput{
			PutObjectBasicInput: obs.PutObjectBasicInput{
				ObjectOperationInput: obs.ObjectOperationInput{
					Bucket:   d.bucket,
					Key:      key,
					Metadata: meta,
				},
				HttpHeader: obs.HttpHeader{
					ContentType: "application/x-directory",
				},
			},
			Body: strings.NewReader(""),
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("obs mkdir %s: %w", relPath, err)
	}
	return nil
}

// Symlink uploads a zero-byte object with the link target stored in metadata.
func (d *Destination) Symlink(ctx context.Context, relPath, target string) error {
	key := d.key(relPath)
	meta := map[string]string{metaSymlinkTarget: target}
	err := withRetry(ctx, d.retryCfg, func() error {
		_, err := d.client.PutObject(&obs.PutObjectInput{
			PutObjectBasicInput: obs.PutObjectBasicInput{
				ObjectOperationInput: obs.ObjectOperationInput{
					Bucket:   d.bucket,
					Key:      key,
					Metadata: meta,
				},
			},
			Body: strings.NewReader(""),
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("obs symlink %s: %w", relPath, err)
	}
	return nil
}

// OpenFile starts an upload and returns a Writer.
func (d *Destination) OpenFile(ctx context.Context, relPath string, size int64, mode os.FileMode, uid, gid int) (storage.Writer, error) {
	key := d.key(relPath)
	meta := posixMetadata(mode, uid, gid)

	if size < int64(smallFileThreshold) {
		return newSmallFileWriter(ctx, d.client, d.bucket, key, meta, d.retryCfg), nil
	}
	return newMultipartFileWriter(ctx, d.client, d.bucket, key, meta, d.retryCfg)
}

// SetMetadata updates an object's metadata using OBS-native SetObjectMetadata
// with REPLACE directive (no self-copy required).
func (d *Destination) SetMetadata(ctx context.Context, relPath string, m model.FileMetadata) error {
	key := d.key(relPath)

	md, err := withRetryResult(ctx, d.retryCfg, func() (*obs.GetObjectMetadataOutput, error) {
		return d.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
			Bucket: d.bucket,
			Key:    key,
		})
	})
	if err != nil {
		return fmt.Errorf("obs head %s: %w", relPath, err)
	}

	merged := md.Metadata
	if merged == nil {
		merged = make(map[string]string)
	}
	if m.Mode != 0 {
		merged[metaMode] = fmt.Sprintf("%04o", m.Mode.Perm())
	}
	if m.Uid != 0 || m.Gid != 0 {
		merged[metaUID] = fmt.Sprintf("%d", m.Uid)
		merged[metaGID] = fmt.Sprintf("%d", m.Gid)
	}
	if m.Atime != 0 {
		merged[metaAtime] = fmt.Sprintf("%d", m.Atime)
	}
	if m.Mtime != 0 {
		merged[metaMtime] = fmt.Sprintf("%d", m.Mtime)
	}
	for k, v := range m.Xattr {
		merged[metaXattrPrefix+k] = v
	}

	err = withRetry(ctx, d.retryCfg, func() error {
		_, err := d.client.SetObjectMetadata(&obs.SetObjectMetadataInput{
			Bucket:            d.bucket,
			Key:               key,
			MetadataDirective: obs.ReplaceMetadata,
			Metadata:          merged,
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("obs setmeta %s: %w", relPath, err)
	}
	return nil
}

// Restat returns metadata for an existing object on the destination
// (for skip-by-mtime).
func (d *Destination) Restat(ctx context.Context, relPath string) (model.DiscoverItem, error) {
	key := d.key(relPath)

	md, err := withRetryResult(ctx, d.retryCfg, func() (*obs.GetObjectMetadataOutput, error) {
		return d.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
			Bucket: d.bucket,
			Key:    key,
		})
	})
	if err != nil {
		return model.DiscoverItem{}, fmt.Errorf("obs restat %s: %w", relPath, err)
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

	return item, nil
}

func posixMetadata(mode os.FileMode, uid, gid int) map[string]string {
	return map[string]string{
		metaMode: fmt.Sprintf("%04o", mode.Perm()),
		metaUID:  fmt.Sprintf("%d", uid),
		metaGID:  fmt.Sprintf("%d", gid),
	}
}

// --- Small file writer (PutObject on Close) ---

type smallFileWriter struct {
	client   *obs.ObsClient
	bucket   string
	key      string
	meta     map[string]string
	buf      bytes.Buffer
	md5Hash  hash.Hash
	retryCfg RetryConfig
	closed   bool
}

func newSmallFileWriter(_ context.Context, client *obs.ObsClient, bucket, key string, meta map[string]string, rc RetryConfig) *smallFileWriter {
	return &smallFileWriter{
		client:   client,
		bucket:   bucket,
		key:      key,
		meta:     meta,
		md5Hash:  md5.New(),
		retryCfg: rc,
	}
}

func (w *smallFileWriter) WriteAt(p []byte, _ int64) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("obs: write on closed writer")
	}
	n, err := w.buf.Write(p)
	if n > 0 {
		w.md5Hash.Write(p[:n])
	}
	return n, err
}

func (w *smallFileWriter) Sync() error { return nil }

func (w *smallFileWriter) Close(ctx context.Context, checksum []byte) error {
	if w.closed {
		return nil
	}
	w.closed = true

	contentMD5 := w.md5Hash.Sum(nil)
	if checksum != nil && !bytes.Equal(checksum, contentMD5) {
		return fmt.Errorf("obs md5 mismatch: client=%x server=%x", checksum, contentMD5)
	}

	if w.meta == nil {
		w.meta = make(map[string]string)
	}
	w.meta[metaMD5] = hex.EncodeToString(contentMD5)

	out, err := withRetryResult(ctx, w.retryCfg, func() (*obs.PutObjectOutput, error) {
		return w.client.PutObject(&obs.PutObjectInput{
			PutObjectBasicInput: obs.PutObjectBasicInput{
				ObjectOperationInput: obs.ObjectOperationInput{
					Bucket:   w.bucket,
					Key:      w.key,
					Metadata: w.meta,
				},
				ContentMD5:    base64.StdEncoding.EncodeToString(contentMD5),
				ContentLength: int64(w.buf.Len()),
			},
			Body: bytes.NewReader(w.buf.Bytes()),
		})
	})
	if err != nil {
		return fmt.Errorf("obs put %s: %w", w.key, err)
	}

	if out != nil && out.ETag != "" {
		etag := strings.ToLower(strings.Trim(out.ETag, `"`))
		if etag != hex.EncodeToString(contentMD5) {
			return fmt.Errorf("obs etag mismatch: etag=%s md5=%x", etag, contentMD5)
		}
	}
	return nil
}

// --- Multipart file writer ---

type multipartFileWriter struct {
	client   *obs.ObsClient
	bucket   string
	key      string
	meta     map[string]string
	uploadID string
	parts    []obs.Part
	partBuf  bytes.Buffer
	partNum  int // OBS uses int (OSS uses int32)
	md5Hash  hash.Hash
	retryCfg RetryConfig
	closed   bool
	ctx      context.Context
}

func newMultipartFileWriter(ctx context.Context, client *obs.ObsClient, bucket, key string, meta map[string]string, rc RetryConfig) (*multipartFileWriter, error) {
	out, err := withRetryResult(ctx, rc, func() (*obs.InitiateMultipartUploadOutput, error) {
		return client.InitiateMultipartUpload(&obs.InitiateMultipartUploadInput{
			ObjectOperationInput: obs.ObjectOperationInput{
				Bucket:   bucket,
				Key:      key,
				Metadata: meta,
			},
		})
	})
	if err != nil {
		return nil, fmt.Errorf("obs initiate multipart %s: %w", key, err)
	}

	return &multipartFileWriter{
		client:   client,
		bucket:   bucket,
		key:      key,
		meta:     meta,
		uploadID: out.UploadId,
		md5Hash:  md5.New(),
		retryCfg: rc,
		ctx:      ctx,
	}, nil
}

func (w *multipartFileWriter) WriteAt(p []byte, _ int64) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("obs: write on closed writer")
	}

	remaining := p
	for len(remaining) > 0 {
		space := minPartSize - w.partBuf.Len()
		n := len(remaining)
		if n > space {
			n = space
		}
		w.partBuf.Write(remaining[:n])
		w.md5Hash.Write(remaining[:n])
		remaining = remaining[n:]

		if w.partBuf.Len() >= minPartSize {
			if err := w.flushPart(); err != nil {
				return len(p) - len(remaining), err
			}
		}
	}
	return len(p), nil
}

func (w *multipartFileWriter) flushPart() error {
	if w.partBuf.Len() == 0 {
		return nil
	}

	w.partNum++
	data := make([]byte, w.partBuf.Len())
	copy(data, w.partBuf.Bytes())
	partMD5 := md5.Sum(data)

	out, err := withRetryResult(w.ctx, w.retryCfg, func() (*obs.UploadPartOutput, error) {
		return w.client.UploadPart(&obs.UploadPartInput{
			Bucket:     w.bucket,
			Key:        w.key,
			UploadId:   w.uploadID,
			PartNumber: w.partNum,
			ContentMD5: base64.StdEncoding.EncodeToString(partMD5[:]),
			Body:       bytes.NewReader(data),
			PartSize:   int64(len(data)),
		})
	})
	if err != nil {
		return fmt.Errorf("obs upload part %d: %w", w.partNum, err)
	}

	w.parts = append(w.parts, obs.Part{
		PartNumber: w.partNum,
		ETag:       out.ETag,
	})
	w.partBuf.Reset()
	return nil
}

func (w *multipartFileWriter) Sync() error { return nil }

func (w *multipartFileWriter) Close(ctx context.Context, checksum []byte) error {
	if w.closed {
		return nil
	}
	w.closed = true
	w.ctx = ctx

	if err := w.flushPart(); err != nil {
		w.abortUpload()
		return err
	}

	if checksum != nil && w.meta != nil {
		w.meta[metaMD5] = hex.EncodeToString(checksum)
	}

	_, err := withRetryResult(w.ctx, w.retryCfg, func() (*obs.CompleteMultipartUploadOutput, error) {
		return w.client.CompleteMultipartUpload(&obs.CompleteMultipartUploadInput{
			Bucket:   w.bucket,
			Key:      w.key,
			UploadId: w.uploadID,
			Parts:    w.parts,
		})
	})
	if err != nil {
		w.abortUpload()
		return fmt.Errorf("obs complete multipart %s: %w", w.key, err)
	}
	return nil
}

func (w *multipartFileWriter) abortUpload() {
	_, _ = w.client.AbortMultipartUpload(&obs.AbortMultipartUploadInput{
		Bucket:   w.bucket,
		Key:      w.key,
		UploadId: w.uploadID,
	})
}
