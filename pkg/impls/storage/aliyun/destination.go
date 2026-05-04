package aliyun

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

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

const (
	minPartSize = 5 << 20 // 5MB — OSS multipart minimum

	metaPrefix = "ncp-"

	metaUID           = metaPrefix + "uid"
	metaGID           = metaPrefix + "gid"
	metaMode          = metaPrefix + "mode"
	metaAtime         = metaPrefix + "atime"
	metaMtime         = metaPrefix + "mtime"
	metaSymlinkTarget = metaPrefix + "symlink-target"
	metaMD5           = metaPrefix + "md5"
	metaXattrPrefix   = metaPrefix + "xattr-"

	smallFileThreshold = 5 << 20 // 5MB — below this, use PutObject
)

// Destination implements storage.Destination for Alibaba Cloud OSS.
type Destination struct {
	client   *oss.Client
	bucket   string
	prefix   string
	retryCfg RetryConfig
}

var _ storage.Destination = (*Destination)(nil)

// Config holds OSS destination configuration.
type Config struct {
	Endpoint string
	Region   string
	AK       string
	SK       string
	Bucket   string
	Prefix   string
	RetryCfg RetryConfig
}

// NewDestination creates an OSS Destination.
func NewDestination(cfg Config) (*Destination, error) {
	ossCfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AK, cfg.SK)).
		WithRegion(cfg.Region).
		WithEndpoint(cfg.Endpoint)
	client := oss.NewClient(ossCfg)

	retryCfg := cfg.RetryCfg
	if retryCfg.MaxAttempts == 0 {
		retryCfg = DefaultRetryConfig()
	}

	return &Destination{
		client:   client,
		bucket:   cfg.Bucket,
		prefix:   cfg.Prefix,
		retryCfg: retryCfg,
	}, nil
}

// Mkdir creates a zero-byte marker object with key ending in "/".
func (d *Destination) Mkdir(ctx context.Context, relPath string, mode os.FileMode, uid, gid int) error {
	key := d.key(relPath + "/")
	meta := posixMetadata(mode, uid, gid)
	err := withRetry(ctx, d.retryCfg, func() error {
		_, err := d.client.PutObject(ctx, &oss.PutObjectRequest{
			Bucket:   oss.Ptr(d.bucket),
			Key:      oss.Ptr(key),
			Body:     strings.NewReader(""),
			Metadata: meta,
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("aliyun mkdir %s: %w", relPath, err)
	}
	return nil
}

// Symlink uploads a zero-byte object with the link target stored in metadata.
func (d *Destination) Symlink(ctx context.Context, relPath string, target string) error {
	key := d.key(relPath)
	meta := map[string]string{metaSymlinkTarget: target}
	err := withRetry(ctx, d.retryCfg, func() error {
		_, err := d.client.PutObject(ctx, &oss.PutObjectRequest{
			Bucket:   oss.Ptr(d.bucket),
			Key:      oss.Ptr(key),
			Body:     strings.NewReader(""),
			Metadata: meta,
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("aliyun symlink %s: %w", relPath, err)
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

// SetMetadata updates an object's metadata using CopyObject with REPLACE directive.
func (d *Destination) SetMetadata(ctx context.Context, relPath string, m model.FileMetadata) error {
	key := d.key(relPath)

	result, err := withRetryResult(ctx, d.retryCfg, func() (*oss.HeadObjectResult, error) {
		return d.client.HeadObject(ctx, &oss.HeadObjectRequest{
			Bucket: oss.Ptr(d.bucket),
			Key:    oss.Ptr(key),
		})
	})
	if err != nil {
		return fmt.Errorf("aliyun head %s: %w", relPath, err)
	}

	merged := result.Metadata
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
		_, err := d.client.CopyObject(ctx, &oss.CopyObjectRequest{
			Bucket:            oss.Ptr(d.bucket),
			Key:               oss.Ptr(key),
			SourceBucket:      oss.Ptr(d.bucket),
			SourceKey:         oss.Ptr(key),
			MetadataDirective: oss.Ptr("REPLACE"),
			Metadata:          merged,
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("aliyun setmeta %s: %w", relPath, err)
	}
	return nil
}

func (d *Destination) key(relPath string) string {
	return d.prefix + relPath
}

// Restat returns metadata for an existing object on the destination (for skip-by-mtime).
func (d *Destination) Restat(ctx context.Context, relPath string) (model.DiscoverItem, error) {
	key := d.key(relPath)

	result, err := withRetryResult(ctx, d.retryCfg, func() (*oss.HeadObjectResult, error) {
		return d.client.HeadObject(ctx, &oss.HeadObjectRequest{
			Bucket: oss.Ptr(d.bucket),
			Key:    oss.Ptr(key),
		})
	})
	if err != nil {
		return model.DiscoverItem{}, fmt.Errorf("aliyun restat %s: %w", relPath, err)
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
		item.Mtime = parseInt64(result.Metadata[metaMtime])
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
	client   *oss.Client
	bucket   string
	key      string
	meta     map[string]string
	buf      bytes.Buffer
	md5Hash  hash.Hash
	retryCfg RetryConfig
	closed   bool
}

func newSmallFileWriter(_ context.Context, client *oss.Client, bucket, key string, meta map[string]string, retryCfg RetryConfig) *smallFileWriter {
	return &smallFileWriter{
		client:   client,
		bucket:   bucket,
		key:      key,
		meta:     meta,
		md5Hash:  md5.New(),
		retryCfg: retryCfg,
	}
}

func (w *smallFileWriter) WriteAt(p []byte, _ int64) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("aliyun: write on closed writer")
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
		return fmt.Errorf("aliyun md5 mismatch: client=%x server=%x", checksum, contentMD5)
	}

	if w.meta == nil {
		w.meta = make(map[string]string)
	}
	w.meta[metaMD5] = hex.EncodeToString(contentMD5)

	result, err := withRetryResult(ctx, w.retryCfg, func() (*oss.PutObjectResult, error) {
		return w.client.PutObject(ctx, &oss.PutObjectRequest{
			Bucket:     oss.Ptr(w.bucket),
			Key:        oss.Ptr(w.key),
			Body:       bytes.NewReader(w.buf.Bytes()),
			ContentMD5: oss.Ptr(base64.StdEncoding.EncodeToString(contentMD5)),
			Metadata:   w.meta,
		})
	})
	if err != nil {
		return fmt.Errorf("aliyun put %s: %w", w.key, err)
	}

	if result.ETag != nil {
		etag := strings.ToLower(strings.Trim(*result.ETag, `"`))
		if etag != hex.EncodeToString(contentMD5) {
			return fmt.Errorf("aliyun etag mismatch: etag=%s md5=%x", etag, contentMD5)
		}
	}
	return nil
}

// --- Multipart file writer ---

type multipartFileWriter struct {
	client   *oss.Client
	bucket   string
	key      string
	meta     map[string]string
	uploadID string
	parts    []oss.UploadPart
	partBuf  bytes.Buffer
	partNum  int32
	md5Hash  hash.Hash
	retryCfg RetryConfig
	closed   bool
	ctx      context.Context
}

func newMultipartFileWriter(ctx context.Context, client *oss.Client, bucket, key string, meta map[string]string, retryCfg RetryConfig) (*multipartFileWriter, error) {
	result, err := withRetryResult(ctx, retryCfg, func() (*oss.InitiateMultipartUploadResult, error) {
		return client.InitiateMultipartUpload(ctx, &oss.InitiateMultipartUploadRequest{
			Bucket:   oss.Ptr(bucket),
			Key:      oss.Ptr(key),
			Metadata: meta,
		})
	})
	if err != nil {
		return nil, fmt.Errorf("aliyun initiate multipart %s: %w", key, err)
	}

	return &multipartFileWriter{
		client:   client,
		bucket:   bucket,
		key:      key,
		meta:     meta,
		uploadID: oss.ToString(result.UploadId),
		md5Hash:  md5.New(),
		retryCfg: retryCfg,
		ctx:      ctx,
	}, nil
}

func (w *multipartFileWriter) WriteAt(p []byte, _ int64) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("aliyun: write on closed writer")
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

	result, err := withRetryResult(w.ctx, w.retryCfg, func() (*oss.UploadPartResult, error) {
		return w.client.UploadPart(w.ctx, &oss.UploadPartRequest{
			Bucket:        oss.Ptr(w.bucket),
			Key:           oss.Ptr(w.key),
			UploadId:      oss.Ptr(w.uploadID),
			PartNumber:    w.partNum,
			Body:          bytes.NewReader(data),
			ContentLength: oss.Ptr(int64(len(data))),
			ContentMD5:    oss.Ptr(base64.StdEncoding.EncodeToString(partMD5[:])),
		})
	})
	if err != nil {
		return fmt.Errorf("aliyun upload part %d: %w", w.partNum, err)
	}

	w.parts = append(w.parts, oss.UploadPart{
		PartNumber: w.partNum,
		ETag:       result.ETag,
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

	_, err := withRetryResult(w.ctx, w.retryCfg, func() (*oss.CompleteMultipartUploadResult, error) {
		return w.client.CompleteMultipartUpload(w.ctx, &oss.CompleteMultipartUploadRequest{
			Bucket:   oss.Ptr(w.bucket),
			Key:      oss.Ptr(w.key),
			UploadId: oss.Ptr(w.uploadID),
			CompleteMultipartUpload: &oss.CompleteMultipartUpload{
				Parts: w.parts,
			},
		})
	})
	if err != nil {
		w.abortUpload()
		return fmt.Errorf("aliyun complete multipart %s: %w", w.key, err)
	}
	return nil
}

func (w *multipartFileWriter) abortUpload() {
	_, _ = w.client.AbortMultipartUpload(w.ctx, &oss.AbortMultipartUploadRequest{
		Bucket:   oss.Ptr(w.bucket),
		Key:      oss.Ptr(w.key),
		UploadId: oss.Ptr(w.uploadID),
	})
}
