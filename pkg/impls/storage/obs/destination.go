package obs

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

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

const (
	defaultPartSize = 100 << 20 // 100MB — multipart upload part size and threshold
)

// Destination implements storage.Destination for Huawei Cloud OBS.
type Destination struct {
	client   *obs.ObsClient
	bucket   string
	prefix   string
	retryCfg RetryConfig
	partSize int64
}

var _ storage.Destination = (*Destination)(nil)

// Config holds OBS destination configuration.
type Config struct {
	Endpoint string
	Region   string
	AK       string
	SK       string
	Bucket   string
	Prefix   string
	RetryCfg RetryConfig
	PartSize int64 // multipart upload part size and threshold; default 100MB
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
	partSize := cfg.PartSize
	if partSize == 0 {
		partSize = defaultPartSize
	}
	return &Destination{
		client:   cli,
		bucket:   cfg.Bucket,
		prefix:   cfg.Prefix,
		retryCfg: rc,
		partSize: partSize,
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
func (d *Destination) OpenFile(ctx context.Context, relPath string, size int64, mode os.FileMode, uid, gid int) (storage.FileWriter, error) {
	key := d.key(relPath)
	meta := posixMetadata(mode, uid, gid)

	if size < d.partSize {
		return newSmallFileWriter(ctx, d.client, d.bucket, key, size, meta, d.retryCfg), nil
	}
	return newMultipartFileWriter(ctx, d.client, d.bucket, key, meta, d.retryCfg, d.partSize)
}

// SetMetadata updates an object's metadata using OBS-native SetObjectMetadata
// with REPLACE directive (no self-copy required).
func (d *Destination) SetMetadata(ctx context.Context, relPath string, attr storage.FileAttr) error {
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
	if attr.Mode != 0 {
		merged[metaMode] = fmt.Sprintf("%04o", osModeToProto(attr.Mode))
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
	if attr.ChecksumHex != "" {
		merged[metaMD5] = attr.ChecksumHex
	}
	if attr.PartSize > 0 {
		merged[metaPartSize] = strconv.FormatInt(attr.PartSize, 10)
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

// Stat returns metadata for an existing object on the destination
// (for skip-by-mtime).
func (d *Destination) Stat(ctx context.Context, relPath string) (storage.DiscoverItem, error) {
	key := d.key(relPath)

	md, err := withRetryResult(ctx, d.retryCfg, func() (*obs.GetObjectMetadataOutput, error) {
		return d.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
			Bucket: d.bucket,
			Key:    key,
		})
	})
	if err != nil {
		return storage.DiscoverItem{}, fmt.Errorf("obs stat %s: %w", relPath, err)
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

	return item, nil
}

// BeginTask is a no-op for OBS destinations.
func (d *Destination) BeginTask(ctx context.Context, taskID string) error { return nil }

// EndTask is a no-op for OBS destinations.
func (d *Destination) EndTask(ctx context.Context, summary storage.TaskSummary) error { return nil }

// ExistsDir checks whether the prefix exists as a directory in OBS.
func (d *Destination) ExistsDir(_ context.Context) (bool, error) {
	// Try HeadObject on the prefix (directory marker)
	_, err := d.client.GetObjectMetadata(&obs.GetObjectMetadataInput{
		Bucket: d.bucket,
		Key:    d.prefix,
	})
	if err == nil {
		return true, nil
	}
	// Fall back to listing objects under the prefix
	result, err := d.client.ListObjects(&obs.ListObjectsInput{
		Bucket: d.bucket,
		ListObjsInput: obs.ListObjsInput{
			Prefix:    d.prefix,
			MaxKeys:   1,
			Delimiter: "/",
		},
	})
	if err != nil {
		return false, fmt.Errorf("obs existsdir list: %w", err)
	}
	return len(result.Contents) > 0 || len(result.CommonPrefixes) > 0, nil
}

// osModeToProto converts Go os.FileMode to POSIX permission bits.
func osModeToProto(mode os.FileMode) uint32 {
	pm := uint32(mode.Perm())
	if mode&os.ModeSetuid != 0 {
		pm |= 0o4000
	}
	if mode&os.ModeSetgid != 0 {
		pm |= 0o2000
	}
	if mode&os.ModeSticky != 0 {
		pm |= 0o1000
	}
	return pm
}

func posixMetadata(mode os.FileMode, uid, gid int) map[string]string {
	return map[string]string{
		metaMode: fmt.Sprintf("%04o", osModeToProto(mode)),
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

func newSmallFileWriter(ctx context.Context, client *obs.ObsClient, bucket, key string, _ int64, meta map[string]string, _ RetryConfig) *smallFileWriter {
	pr, pw := io.Pipe()
	done := make(chan error, 1)

	go func() {
		_, err := client.PutObject(&obs.PutObjectInput{
			PutObjectBasicInput: obs.PutObjectBasicInput{
				ObjectOperationInput: obs.ObjectOperationInput{
					Bucket:   bucket,
					Key:      key,
					Metadata: meta,
				},
			},
			Body: pr,
		})
		done <- err
	}()

	return &smallFileWriter{pw: pw, done: done, md5: md5.New()}
}

func (w *smallFileWriter) Write(_ context.Context, p []byte) (int, error) {
	if w.state != stateOpen {
		return 0, fmt.Errorf("obs: write on closed writer")
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
		return fmt.Errorf("obs put: %w", err)
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
	client   *obs.ObsClient
	bucket   string
	key      string
	meta     map[string]string
	uploadID string
	parts    []obs.Part
	partBuf  bytes.Buffer
	partNum  int // OBS uses int (OSS uses int32)
	partSize int64
	md5Hash  hash.Hash
	retryCfg RetryConfig
	state    writerState
	ctx      context.Context
}

func newMultipartFileWriter(ctx context.Context, client *obs.ObsClient, bucket, key string, meta map[string]string, rc RetryConfig, partSize int64) (*multipartFileWriter, error) {
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
		partSize: partSize,
		md5Hash:  md5.New(),
		retryCfg: rc,
		ctx:      ctx,
	}, nil
}

func (w *multipartFileWriter) Write(_ context.Context, p []byte) (int, error) {
	if w.state != stateOpen {
		return 0, fmt.Errorf("obs: write on closed writer")
	}

	remaining := p
	for len(remaining) > 0 {
		space := w.partSize - int64(w.partBuf.Len())
		n := len(remaining)
		if int64(n) > space {
			n = int(space)
		}
		w.partBuf.Write(remaining[:n])
		w.md5Hash.Write(remaining[:n])
		remaining = remaining[n:]

		if int64(w.partBuf.Len()) >= w.partSize {
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
	_, _ = w.client.AbortMultipartUpload(&obs.AbortMultipartUploadInput{
		Bucket:   w.bucket,
		Key:      w.key,
		UploadId: w.uploadID,
	})
}
