package di

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/zp001/ncp/pkg/impls/storage/aliyun"
	"github.com/zp001/ncp/pkg/impls/storage/cos"
	"github.com/zp001/ncp/pkg/impls/storage/local"
	"github.com/zp001/ncp/pkg/impls/storage/obs"
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

// NewSource creates a Source from srcPath. Cloud schemes require a profile
// reference embedded in the URL userinfo (e.g. oss://prod@bkt/path); local
// and ncp:// schemes must NOT carry userinfo.
func NewSource(srcPath string, profiles map[string]model.Profile) (storage.Source, error) {
	u, err := ParsePath(srcPath)
	if err != nil {
		return nil, err
	}
	prof, err := resolveProfile(u, profiles)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "", "file":
		return local.NewSource(u.Path)
	case "ncp":
		return remote.NewSource(u.Host, u.Path)
	case "oss":
		bucket, prefix := parseOSSURL(u)
		if bucket == "" {
			return nil, fmt.Errorf("oss: bucket name required in URL (oss://<profile>@bucket/prefix)")
		}
		return aliyun.NewSource(aliyun.SourceConfig{
			Endpoint: prof.Endpoint,
			Region:   prof.Region,
			AK:       prof.AK,
			SK:       prof.SK,
			Bucket:   bucket,
			Prefix:   prefix,
		})
	case "cos":
		bucket, prefix := parseCOSURL(u)
		if bucket == "" {
			return nil, fmt.Errorf("cos: bucket name required in URL (cos://<profile>@bucket/prefix)")
		}
		return cos.NewSource(cos.SourceConfig{
			Endpoint: prof.Endpoint,
			Region:   prof.Region,
			AK:       prof.AK,
			SK:       prof.SK,
			Bucket:   bucket,
			Prefix:   prefix,
		})
	case "obs":
		bucket, prefix := parseOBSURL(u)
		if bucket == "" {
			return nil, fmt.Errorf("obs: bucket name required in URL (obs://<profile>@bucket/prefix)")
		}
		return obs.NewSource(obs.SourceConfig{
			Endpoint: prof.Endpoint,
			Region:   prof.Region,
			AK:       prof.AK,
			SK:       prof.SK,
			Bucket:   bucket,
			Prefix:   prefix,
		})
	default:
		return nil, fmt.Errorf("unsupported source scheme: %s", u.Scheme)
	}
}

// NewDestination creates a Destination from dstPath. Profile rules match NewSource.
func NewDestination(dstPath string, cfg DestConfig, profiles map[string]model.Profile) (storage.Destination, error) {
	u, err := ParsePath(dstPath)
	if err != nil {
		return nil, err
	}
	prof, err := resolveProfile(u, profiles)
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
		bucket, prefix := parseOSSURL(u)
		if bucket == "" {
			return nil, fmt.Errorf("oss: bucket name required in URL (oss://<profile>@bucket/prefix)")
		}
		return aliyun.NewDestination(aliyun.Config{
			Endpoint: prof.Endpoint,
			Region:   prof.Region,
			AK:       prof.AK,
			SK:       prof.SK,
			Bucket:   bucket,
			Prefix:   prefix,
		})
	case "cos":
		bucket, prefix := parseCOSURL(u)
		if bucket == "" {
			return nil, fmt.Errorf("cos: bucket name required in URL (cos://<profile>@bucket/prefix)")
		}
		return cos.NewDestination(cos.Config{
			Endpoint: prof.Endpoint,
			Region:   prof.Region,
			AK:       prof.AK,
			SK:       prof.SK,
			Bucket:   bucket,
			Prefix:   prefix,
		})
	case "obs":
		bucket, prefix := parseOBSURL(u)
		if bucket == "" {
			return nil, fmt.Errorf("obs: bucket name required in URL (obs://<profile>@bucket/prefix)")
		}
		return obs.NewDestination(obs.Config{
			Endpoint: prof.Endpoint,
			Region:   prof.Region,
			AK:       prof.AK,
			SK:       prof.SK,
			Bucket:   bucket,
			Prefix:   prefix,
		})
	default:
		return nil, fmt.Errorf("unsupported destination scheme: %s", u.Scheme)
	}
}

// resolveProfile validates that the URL's profile reference (or absence of one)
// is consistent with the scheme, then returns the matching profile.
//
// Rules:
//   - Local / ncp:// URLs MUST NOT carry userinfo.
//   - Cloud URLs (per model.CloudSchemes) MUST carry a profile name in userinfo,
//     no password, and the named profile must exist in `profiles` with a
//     matching Provider.
//
// Returns (nil, nil) when the URL needs no profile (local / ncp).
func resolveProfile(u *url.URL, profiles map[string]model.Profile) (*model.Profile, error) {
	if !model.IsCloudScheme(u.Scheme) {
		if u.User != nil {
			return nil, fmt.Errorf("scheme %q does not accept a profile, but URL contains userinfo %q",
				u.Scheme, u.User.Username())
		}
		return nil, nil
	}

	if u.User == nil {
		return nil, fmt.Errorf("scheme %q requires a profile; use form: %s://<profile>@<bucket>/<path>",
			u.Scheme, u.Scheme)
	}
	if _, hasPwd := u.User.Password(); hasPwd {
		return nil, fmt.Errorf("scheme %q: embedding password in URL is not allowed; reference a profile name only",
			u.Scheme)
	}
	name := u.User.Username()
	if name == "" {
		return nil, fmt.Errorf("scheme %q: empty profile name in URL", u.Scheme)
	}
	prof, ok := profiles[name]
	if !ok {
		return nil, fmt.Errorf("profile %q referenced in URL is not defined in ncp_config.json", name)
	}
	if err := prof.Validate(name, u.Scheme); err != nil {
		return nil, err
	}
	return &prof, nil
}

// parseOSSURL extracts bucket and prefix from an oss:// URL.
// oss://prod@mybucket/path/to/dir → bucket="mybucket", prefix="path/to/dir/"
func parseOSSURL(u *url.URL) (bucket, prefix string) {
	bucket = u.Host
	prefix = strings.TrimPrefix(u.Path, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return bucket, prefix
}

// parseCOSURL extracts bucket and prefix from a cos:// URL.
// cos://prod@mybucket-1250000000/path/to/dir → bucket="mybucket-1250000000", prefix="path/to/dir/"
func parseCOSURL(u *url.URL) (bucket, prefix string) {
	bucket = u.Host
	prefix = strings.TrimPrefix(u.Path, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return bucket, prefix
}

// parseOBSURL extracts bucket and prefix from an obs:// URL.
// obs://prod@mybucket/path/to/dir → bucket="mybucket", prefix="path/to/dir/"
func parseOBSURL(u *url.URL) (bucket, prefix string) {
	bucket = u.Host
	prefix = strings.TrimPrefix(u.Path, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return bucket, prefix
}

// ParsePath turns a string into a *url.URL. Local paths (no "://") are
// resolved to absolute paths and returned with empty scheme.
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
