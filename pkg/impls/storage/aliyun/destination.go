package aliyun

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
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
)

// Destination implements storage.Destination for Alibaba Cloud OSS.
type Destination struct {
	client             *oss.Client
	bucket             string
	prefix             string
	retryCfg           RetryConfig
	multipartThreshold int64
}

var _ storage.Destination = (*Destination)(nil)

// Config holds OSS destination configuration.
type Config struct {
	Endpoint           string
	Region             string
	AK                 string
	SK                 string
	Bucket             string
	Prefix             string
	RetryCfg           RetryConfig
	MultipartThreshold int64 // default 1GB; must be >= minPartSize
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

	threshold := cfg.MultipartThreshold
	if threshold == 0 {
		threshold = 1 << 30 // 1GB default
	}

	return &Destination{
		client:             client,
		bucket:             cfg.Bucket,
		prefix:             cfg.Prefix,
		retryCfg:           retryCfg,
		multipartThreshold: threshold,
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
func (d *Destination) OpenFile(ctx context.Context, relPath string, size int64, mode os.FileMode, uid, gid int) (storage.FileWriter, error) {
	key := d.key(relPath)
	meta := posixMetadata(mode, uid, gid)

	if size < d.multipartThreshold {
		return newSmallFileWriter(ctx, d.client, d.bucket, key, size, meta, d.retryCfg), nil
	}
	return newMultipartFileWriter(ctx, d.client, d.bucket, key, meta, d.retryCfg)
}

// SetMetadata updates an object's metadata using CopyObject with REPLACE directive.
func (d *Destination) SetMetadata(ctx context.Context, relPath string, attr storage.FileAttr) error {
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
	if attr.Mode != 0 {
		merged[metaMode] = fmt.Sprintf("%04o", attr.Mode.Perm())
	}
	if attr.Uid != 0 || attr.Gid != 0 {
		merged[metaUID] = fmt.Sprintf("%d", attr.Uid)
		merged[metaGID] = fmt.Sprintf("%d", attr.Gid)
	}
	if !attr.Atime.IsZero() {
		merged[metaAtime] = strconv.FormatInt(attr.Atime.UnixNano(), 10)
	}
	if !attr.Mtime.IsZero() {
		merged[metaMtime] = strconv.FormatInt(attr.Mtime.UnixNano(), 10)
	}
	for k, v := range attr.Xattr {
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

// Connect is a no-op for OSS destinations (client is initialized in constructor).
func (d *Destination) Connect(ctx context.Context) error { return nil }

// Close is a no-op for OSS destinations.
func (d *Destination) Close(ctx context.Context) error { return nil }

// BeginTask is a no-op for OSS destinations.
func (d *Destination) BeginTask(ctx context.Context, taskID string) error { return nil }

// EndTask is a no-op for OSS destinations.
func (d *Destination) EndTask(ctx context.Context, summary storage.TaskSummary) error { return nil }

// Stat returns metadata for an existing object on the destination (for skip-by-mtime).
func (d *Destination) Stat(ctx context.Context, relPath string) (storage.DiscoverItem, error) {
	key := d.key(relPath)

	result, err := withRetryResult(ctx, d.retryCfg, func() (*oss.HeadObjectResult, error) {
		return d.client.HeadObject(ctx, &oss.HeadObjectRequest{
			Bucket: oss.Ptr(d.bucket),
			Key:    oss.Ptr(key),
		})
	})
	if err != nil {
		return storage.DiscoverItem{}, fmt.Errorf("aliyun stat %s: %w", relPath, err)
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
		if ft == model.FileSymlink {
			item.Attr.SymlinkTarget = result.Metadata[metaSymlinkTarget]
		}
		if mtime := parseInt64(result.Metadata[metaMtime]); mtime != 0 {
			item.Attr.Mtime = time.Unix(0, mtime)
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

// --- Small file writer (io.Pipe streaming PutObject) ---

type writerState int

const (
	stateOpen writerState = iota
	stateCommitted
	stateAborted
)

type smallFileWriter struct {
	pw    *io.PipeWriter
	done  chan error
	md5   hash.Hash
	state writerState
}

func newSmallFileWriter(ctx context.Context, client *oss.Client, bucket, key string, size int64, meta map[string]string, _ RetryConfig) *smallFileWriter {
	pr, pw := io.Pipe()
	done := make(chan error, 1)

	go func() {
		req := &oss.PutObjectRequest{
			Bucket:     oss.Ptr(bucket),
			Key:        oss.Ptr(key),
			Body:       pr,
			ContentLength: oss.Ptr(size),
			Metadata:   meta,
		}
		_, err := client.PutObject(ctx, req)
		done <- err
	}()

	return &smallFileWriter{pw: pw, done: done, md5: md5.New()}
}

func (w *smallFileWriter) Write(_ context.Context, p []byte) (int, error) {
	if w.state != stateOpen {
		return 0, fmt.Errorf("aliyun: write on closed writer")
	}
	n, err := w.pw.Write(p)
	if n > 0 {
		w.md5.Write(p[:n])
	}
	return n, err
}

func (w *smallFileWriter) Commit(_ context.Context, checksum []byte) error {
	if w.state != stateOpen {
		return nil
	}
	w.state = stateCommitted
	w.pw.Close()

	if err := <-w.done; err != nil {
		return fmt.Errorf("aliyun put: %w", err)
	}
	if checksum != nil && !bytes.Equal(checksum, w.md5.Sum(nil)) {
		return storage.ErrChecksum
	}
	return nil
}

func (w *smallFileWriter) Abort(_ context.Context) error {
	if w.state != stateOpen {
		return nil
	}
	w.state = stateAborted
	w.pw.CloseWithError(io.ErrClosedPipe)
	<-w.done
	return nil
}

func (w *smallFileWriter) BytesWritten() int64 { return 0 } // not tracked for small files

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
	state    writerState
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

func (w *multipartFileWriter) Write(_ context.Context, p []byte) (int, error) {
	if w.state != stateOpen {
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
	data := w.partBuf.Bytes()
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

func (w *multipartFileWriter) Commit(ctx context.Context, checksum []byte) error {
	if w.state != stateOpen {
		return nil
	}
	w.state = stateCommitted
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

func (w *multipartFileWriter) Abort(_ context.Context) error {
	if w.state != stateOpen {
		return nil
	}
	w.state = stateAborted
	w.abortUpload()
	return nil
}

func (w *multipartFileWriter) BytesWritten() int64 { return 0 }

func (w *multipartFileWriter) abortUpload() {
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(w.ctx), 30*time.Second)
	defer cancel()
	_, _ = w.client.AbortMultipartUpload(abortCtx, &oss.AbortMultipartUploadRequest{
		Bucket:   oss.Ptr(w.bucket),
		Key:      oss.Ptr(w.key),
		UploadId: oss.Ptr(w.uploadID),
	})
}
