package storage

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/zp001/ncp/internal/storage/local"
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
func NewSource(srcPath string) (Source, error) {
	u, err := parsePath(srcPath)
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
func NewDestination(dstPath string) (Destination, error) {
	return NewDestinationWithConfig(dstPath, DestConfig{})
}

// NewDestinationWithConfig creates a Destination with IO configuration.
func NewDestinationWithConfig(dstPath string, cfg DestConfig) (Destination, error) {
	u, err := parsePath(dstPath)
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
			// Neither specified: default SyncWrites=true
			wcfg.SyncWrites = true
		}
		return local.NewDestinationWithConfig(u.Path, wcfg)
	default:
		return nil, fmt.Errorf("unsupported destination scheme: %s", u.Scheme)
	}
}

func parsePath(p string) (*url.URL, error) {
	if !strings.Contains(p, "://") {
		abs, err := filepath.Abs(p)
		if err != nil {
			return nil, fmt.Errorf("abs path: %w", err)
		}
		return &url.URL{Scheme: "", Path: abs}, nil
	}
	return url.Parse(p)
}
