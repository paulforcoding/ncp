package di

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/zp001/ncp/pkg/impls/storage/aliyun"
	"github.com/zp001/ncp/pkg/impls/storage/local"
	"github.com/zp001/ncp/pkg/impls/storage/remote"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// DestConfig holds IO configuration for destination creation.
type DestConfig struct {
	DirectIO    bool
	SyncWrites  bool
	IOSize      int
	IOSizeTiers []model.IOSizeTier
}

// OSSConfig holds Alibaba Cloud OSS configuration.
type OSSConfig struct {
	Endpoint string
	Region   string
	AK       string
	SK       string
}

// NewSource creates a Source based on the URL scheme of srcPath.
func NewSource(srcPath string) (storage.Source, error) {
	return NewSourceWithOSS(srcPath, OSSConfig{})
}

// NewSourceWithOSS creates a Source with optional OSS configuration.
func NewSourceWithOSS(srcPath string, ossCfg OSSConfig) (storage.Source, error) {
	u, err := ParsePath(srcPath)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "", "file":
		return local.NewSource(u.Path)
	case "ncp":
		return remote.NewSource(u.Host, u.Path)
	case "oss":
		return newOSSSource(u, ossCfg)
	default:
		return nil, fmt.Errorf("unsupported source scheme: %s", u.Scheme)
	}
}

// NewDestination creates a Destination based on the URL scheme of dstPath.
func NewDestination(dstPath string) (storage.Destination, error) {
	return NewDestinationWithConfig(dstPath, DestConfig{}, OSSConfig{})
}

// NewDestinationWithConfig creates a Destination with IO configuration.
func NewDestinationWithConfig(dstPath string, cfg DestConfig, ossCfg ...OSSConfig) (storage.Destination, error) {
	u, err := ParsePath(dstPath)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "", "file":
		wcfg := local.WriterConfig{
			DirectIO:    cfg.DirectIO,
			SyncWrites:  cfg.SyncWrites,
			IOSize:      cfg.IOSize,
			IOSizeTiers: cfg.IOSizeTiers,
		}
		if wcfg.IOSizeTiers == nil {
			wcfg.IOSizeTiers = model.DefaultIOSizeTiers()
		}
		if !cfg.DirectIO && !cfg.SyncWrites {
			wcfg.SyncWrites = true
		}
		return local.NewDestinationWithConfig(u.Path, wcfg)
	case "ncp":
		return remote.NewDestination(u.Host, u.Path)
	case "oss":
		var oc OSSConfig
		if len(ossCfg) > 0 {
			oc = ossCfg[0]
		}
		return newOSSDestination(u, oc)
	default:
		return nil, fmt.Errorf("unsupported destination scheme: %s", u.Scheme)
	}
}

func newOSSDestination(u *url.URL, cfg OSSConfig) (*aliyun.Destination, error) {
	bucket, prefix := parseOSSURL(u)
	if bucket == "" {
		return nil, fmt.Errorf("oss: bucket name required in URL (oss://bucket/prefix)")
	}
	return aliyun.NewDestination(aliyun.Config{
		Endpoint: cfg.Endpoint,
		Region:   cfg.Region,
		AK:       cfg.AK,
		SK:       cfg.SK,
		Bucket:   bucket,
		Prefix:   prefix,
	})
}

func newOSSSource(u *url.URL, cfg OSSConfig) (*aliyun.Source, error) {
	bucket, prefix := parseOSSURL(u)
	if bucket == "" {
		return nil, fmt.Errorf("oss: bucket name required in URL (oss://bucket/prefix)")
	}
	return aliyun.NewSource(aliyun.SourceConfig{
		Endpoint: cfg.Endpoint,
		Region:   cfg.Region,
		AK:       cfg.AK,
		SK:       cfg.SK,
		Bucket:   bucket,
		Prefix:   prefix,
	})
}

// parseOSSURL extracts bucket and prefix from an oss:// URL.
// oss://mybucket/path/to/dir → bucket="mybucket", prefix="path/to/dir/"
func parseOSSURL(u *url.URL) (bucket, prefix string) {
	bucket = u.Host
	prefix = strings.TrimPrefix(u.Path, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return bucket, prefix
}

func ParsePath(p string) (*url.URL, error) {
	if !strings.Contains(p, "://") {
		abs, err := filepath.Abs(p)
		if err != nil {
			return nil, fmt.Errorf("abs path: %w", err)
		}
		return &url.URL{Scheme: "", Path: abs}, nil
	}
	return url.Parse(p)
}

// NewRemoteDestination creates a remote Destination for ncp:// URLs.
func NewRemoteDestination(addr, basePath string) (storage.Destination, error) {
	return remote.NewDestination(addr, basePath)
}
