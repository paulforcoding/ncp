package cos

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/tencentyun/cos-go-sdk-v5"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

const (
	minPartSize = 1 << 20 // 1MB — COS multipart minimum

	metaPrefix = "ncp-"

	metaUID           = metaPrefix + "uid"
	metaGID           = metaPrefix + "gid"
	metaMode          = metaPrefix + "mode"
	metaAtime         = metaPrefix + "atime"
	metaMtime         = metaPrefix + "mtime"
	metaSymlinkTarget = metaPrefix + "symlink-target"
	metaMD5           = metaPrefix + "md5"
	metaXattrPrefix   = metaPrefix + "xattr-"

	smallFileThreshold = 1 << 20 // 1MB — below this, use PutObject
)

// Destination implements storage.Destination for Tencent Cloud COS.
type Destination struct {
	client   *cos.Client
	bucket   string
	prefix   string
	retryCfg RetryConfig
}

var _ storage.Destination = (*Destination)(nil)
var _ storage.Restatter = (*Destination)(nil)

// Config holds COS destination configuration.
type Config struct {
	Endpoint string
	Region   string
	AK       string
	SK       string
	Bucket   string
	Prefix   string
	RetryCfg RetryConfig
}

// NewDestination creates a COS Destination.
func NewDestination(cfg Config) (*Destination, error) {
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

func (d *Destination) key(relPath string) string {
	return d.prefix + relPath
}

// buildMetaHeader converts a map of metadata into an *http.Header for XCosMetaXXX.
func buildMetaHeader(meta map[string]string) *http.Header {
	h := make(http.Header)
	for k, v := range meta {
		h.Set(k, v)
	}
	return &h
}

// Mkdir creates a zero-byte marker object with key ending in "/".
func (d *Destination) Mkdir(ctx context.Context, relPath string, mode os.FileMode, uid, gid int) error {
	key := d.key(relPath + "/")
	meta := posixMetadata(mode, uid, gid)

	err := withRetry(ctx, d.retryCfg, func() error {
		_, err := d.client.Object.Put(ctx, key, strings.NewReader(""), &cos.ObjectPutOptions{
			ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
				ContentType: "application/x-directory",
				XCosMetaXXX: buildMetaHeader(meta),
			},
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("cos mkdir %s: %w", relPath, err)
	}
	return nil
}

// Symlink uploads a zero-byte object with the link target stored in metadata.
func (d *Destination) Symlink(ctx context.Context, relPath string, target string) error {
	key := d.key(relPath)
	meta := map[string]string{metaSymlinkTarget: target}

	err := withRetry(ctx, d.retryCfg, func() error {
		_, err := d.client.Object.Put(ctx, key, strings.NewReader(""), &cos.ObjectPutOptions{
			ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
				XCosMetaXXX: buildMetaHeader(meta),
			},
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("cos symlink %s: %w", relPath, err)
	}
	return nil
}

// OpenFile starts an upload and returns a Writer.
func (d *Destination) OpenFile(ctx context.Context, relPath string, size int64, mode os.FileMode, uid, gid int) (storage.Writer, error) {
	key := d.key(relPath)
	meta := posixMetadata(mode, uid, gid)

	if size < int64(smallFileThreshold) {
		return newSmallFileWriter(ctx, d.client, key, meta, d.retryCfg), nil
	}
	return newMultipartFileWriter(ctx, d.client, key, meta, d.retryCfg)
}

// SetMetadata updates an object's metadata using CopyObject with REPLACE directive.
func (d *Destination) SetMetadata(ctx context.Context, relPath string, m model.FileMetadata) error {
	key := d.key(relPath)

	resp, err := withRetryResult(ctx, d.retryCfg, func() (*cos.Response, error) {
		return d.client.Object.Head(ctx, key, nil)
	})
	if err != nil {
		return fmt.Errorf("cos head %s: %w", relPath, err)
	}

	merged := extractMetadata(resp.Header)
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

	sourceURL := fmt.Sprintf("%s/%s", d.client.BaseURL.BucketURL.Host, key)

	err = withRetry(ctx, d.retryCfg, func() error {
		_, _, err := d.client.Object.Copy(ctx, key, sourceURL, &cos.ObjectCopyOptions{
			ObjectCopyHeaderOptions: &cos.ObjectCopyHeaderOptions{
				XCosMetadataDirective: "REPLACE",
				XCosMetaXXX:           buildMetaHeader(merged),
			},
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("cos setmeta %s: %w", relPath, err)
	}
	return nil
}

// Restat returns metadata for an existing object on the destination (for skip-by-mtime).
func (d *Destination) Restat(ctx context.Context, relPath string) (model.DiscoverItem, error) {
	key := d.key(relPath)

	resp, err := withRetryResult(ctx, d.retryCfg, func() (*cos.Response, error) {
		return d.client.Object.Head(ctx, key, nil)
	})
	if err != nil {
		return model.DiscoverItem{}, fmt.Errorf("cos restat %s: %w", relPath, err)
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
	client   *cos.Client
	key      string
	meta     map[string]string
	buf      bytes.Buffer
	md5Hash  hash.Hash
	retryCfg RetryConfig
	closed   bool
}

func newSmallFileWriter(_ context.Context, client *cos.Client, key string, meta map[string]string, retryCfg RetryConfig) *smallFileWriter {
	return &smallFileWriter{
		client:   client,
		key:      key,
		meta:     meta,
		md5Hash:  md5.New(),
		retryCfg: retryCfg,
	}
}

func (w *smallFileWriter) WriteAt(p []byte, _ int64) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("cos: write on closed writer")
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
		return fmt.Errorf("cos md5 mismatch: client=%x server=%x", checksum, contentMD5)
	}

	if w.meta == nil {
		w.meta = make(map[string]string)
	}
	w.meta[metaMD5] = hex.EncodeToString(contentMD5)

	resp, err := withRetryResult(ctx, w.retryCfg, func() (*cos.Response, error) {
		return w.client.Object.Put(ctx, w.key, bytes.NewReader(w.buf.Bytes()), &cos.ObjectPutOptions{
			ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
				ContentMD5: base64.StdEncoding.EncodeToString(contentMD5),
				XCosMetaXXX: buildMetaHeader(w.meta),
			},
		})
	})
	if err != nil {
		return fmt.Errorf("cos put %s: %w", w.key, err)
	}

	if resp != nil {
		etag := strings.ToLower(strings.Trim(resp.Header.Get("ETag"), `"`))
		if etag != "" && etag != hex.EncodeToString(contentMD5) {
			return fmt.Errorf("cos etag mismatch: etag=%s md5=%x", etag, contentMD5)
		}
	}
	return nil
}

// --- Multipart file writer ---

type multipartFileWriter struct {
	client   *cos.Client
	key      string
	meta     map[string]string
	uploadID string
	parts    []cos.Object
	partBuf  bytes.Buffer
	partNum  int
	md5Hash  hash.Hash
	retryCfg RetryConfig
	closed   bool
	ctx      context.Context
}

func newMultipartFileWriter(ctx context.Context, client *cos.Client, key string, meta map[string]string, retryCfg RetryConfig) (*multipartFileWriter, error) {
	var result *cos.InitiateMultipartUploadResult
	err := withRetry(ctx, retryCfg, func() error {
		r, _, e := client.Object.InitiateMultipartUpload(ctx, key, &cos.InitiateMultipartUploadOptions{
			ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
				XCosMetaXXX: buildMetaHeader(meta),
			},
		})
		if e != nil {
			return e
		}
		result = r
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("cos initiate multipart %s: %w", key, err)
	}

	return &multipartFileWriter{
		client:   client,
		key:      key,
		meta:     meta,
		uploadID: result.UploadID,
		md5Hash:  md5.New(),
		retryCfg: retryCfg,
		ctx:      ctx,
	}, nil
}

func (w *multipartFileWriter) WriteAt(p []byte, _ int64) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("cos: write on closed writer")
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

	resp, err := withRetryResult(w.ctx, w.retryCfg, func() (*cos.Response, error) {
		return w.client.Object.UploadPart(w.ctx, w.key, w.uploadID, w.partNum,
			bytes.NewReader(data), &cos.ObjectUploadPartOptions{
				ContentLength: int64(len(data)),
				ContentMD5:    base64.StdEncoding.EncodeToString(partMD5[:]),
			})
	})
	if err != nil {
		return fmt.Errorf("cos upload part %d: %w", w.partNum, err)
	}

	etag := ""
	if resp != nil {
		etag = strings.ToLower(strings.Trim(resp.Header.Get("ETag"), `"`))
	}

	w.parts = append(w.parts, cos.Object{
		Key:        w.key,
		ETag:       etag,
		PartNumber: w.partNum,
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

	opt := &cos.CompleteMultipartUploadOptions{
		Parts: w.parts,
	}

	err := withRetry(w.ctx, w.retryCfg, func() error {
		_, _, e := w.client.Object.CompleteMultipartUpload(w.ctx, w.key, w.uploadID, opt)
		return e
	})
	if err != nil {
		w.abortUpload()
		return fmt.Errorf("cos complete multipart %s: %w", w.key, err)
	}
	return nil
}

func (w *multipartFileWriter) abortUpload() {
	_, _ = w.client.Object.AbortMultipartUpload(w.ctx, w.key, w.uploadID)
}
