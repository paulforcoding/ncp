package cos

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
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

)

// Destination implements storage.Destination for Tencent Cloud COS.
type Destination struct {
	client             *cos.Client
	bucket             string
	prefix             string
	retryCfg           RetryConfig
	multipartThreshold int64
}

var _ storage.Destination = (*Destination)(nil)

// Config holds COS destination configuration.
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
func (d *Destination) OpenFile(ctx context.Context, relPath string, size int64, mode os.FileMode, uid, gid int) (storage.FileWriter, error) {
	key := d.key(relPath)
	meta := posixMetadata(mode, uid, gid)

	if size < d.multipartThreshold {
		return newSmallFileWriter(ctx, d.client, key, size, meta, d.retryCfg), nil
	}
	return newMultipartFileWriter(ctx, d.client, key, meta, d.retryCfg)
}

// SetMetadata updates an object's metadata using CopyObject with REPLACE directive.
func (d *Destination) SetMetadata(ctx context.Context, relPath string, attr storage.FileAttr) error {
	key := d.key(relPath)

	resp, err := withRetryResult(ctx, d.retryCfg, func() (*cos.Response, error) {
		return d.client.Object.Head(ctx, key, nil)
	})
	if err != nil {
		return fmt.Errorf("cos head %s: %w", relPath, err)
	}

	merged := extractMetadata(resp.Header)
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

// Connect is a no-op for COS destinations (client is initialized in constructor).
func (d *Destination) Connect(ctx context.Context) error { return nil }

// Close is a no-op for COS destinations.
func (d *Destination) Close(ctx context.Context) error { return nil }

// BeginTask is a no-op for COS destinations.
func (d *Destination) BeginTask(ctx context.Context, taskID string) error { return nil }

// EndTask is a no-op for COS destinations.
func (d *Destination) EndTask(ctx context.Context, summary storage.TaskSummary) error { return nil }

// Stat returns metadata for an existing object on the destination (for skip-by-mtime).
func (d *Destination) Stat(ctx context.Context, relPath string) (storage.DiscoverItem, error) {
	key := d.key(relPath)

	resp, err := withRetryResult(ctx, d.retryCfg, func() (*cos.Response, error) {
		return d.client.Object.Head(ctx, key, nil)
	})
	if err != nil {
		return storage.DiscoverItem{}, fmt.Errorf("cos stat %s: %w", relPath, err)
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

func newSmallFileWriter(ctx context.Context, client *cos.Client, key string, size int64, meta map[string]string, _ RetryConfig) *smallFileWriter {
	pr, pw := io.Pipe()
	done := make(chan error, 1)

	go func() {
		_, err := client.Object.Put(ctx, key, pr, &cos.ObjectPutOptions{
			ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
				XCosMetaXXX: buildMetaHeader(meta),
			},
		})
		done <- err
	}()

	return &smallFileWriter{pw: pw, done: done, md5: md5.New()}
}

func (w *smallFileWriter) Write(_ context.Context, p []byte) (int, error) {
	if w.state != stateOpen {
		return 0, fmt.Errorf("cos: write on closed writer")
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
		return fmt.Errorf("cos put: %w", err)
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

func (w *smallFileWriter) BytesWritten() int64 { return 0 }

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
	state    writerState
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

func (w *multipartFileWriter) Write(_ context.Context, p []byte) (int, error) {
	if w.state != stateOpen {
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
	data := w.partBuf.Bytes()
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
	_, _ = w.client.Object.AbortMultipartUpload(abortCtx, w.key, w.uploadID)
}
