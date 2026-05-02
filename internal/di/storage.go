package di

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

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

// NewSource creates a Source based on the URL scheme of srcPath.
func NewSource(srcPath string) (storage.Source, error) {
	u, err := ParsePath(srcPath)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "", "file":
		return local.NewSource(u.Path)
	default:
		return nil, fmt.Errorf("unsupported source scheme: %s", u.Scheme)
	}
}

// NewDestination creates a Destination based on the URL scheme of dstPath.
func NewDestination(dstPath string) (storage.Destination, error) {
	return NewDestinationWithConfig(dstPath, DestConfig{})
}

// NewDestinationWithConfig creates a Destination with IO configuration.
func NewDestinationWithConfig(dstPath string, cfg DestConfig) (storage.Destination, error) {
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
	default:
		return nil, fmt.Errorf("unsupported destination scheme: %s", u.Scheme)
	}
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
