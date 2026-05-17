package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/zp001/ncp/pkg/model"
)

// Config holds all ncp configuration.
type Config struct {
	CopyParallelism    int                `json:"CopyParallelism" mapstructure:"CopyParallelism"`
	ProgramLogLevel    string             `json:"ProgramLogLevel" mapstructure:"ProgramLogLevel"`
	ProgramLogOutput   string             `json:"ProgramLogOutput" mapstructure:"ProgramLogOutput"`
	FileLogEnabled     bool               `json:"FileLogEnabled" mapstructure:"FileLogEnabled"`
	FileLogOutput      string             `json:"FileLogOutput" mapstructure:"FileLogOutput"`
	FileLogInterval    int                `json:"FileLogInterval" mapstructure:"FileLogInterval"`
	DirectIO           bool               `json:"DirectIO" mapstructure:"DirectIO"`
	SyncWrites         bool               `json:"SyncWrites" mapstructure:"SyncWrites"`
	IOSize             int                `json:"IOSize" mapstructure:"IOSize"`
	IOSizeTiers        []model.IOSizeTier `json:"IOSizeTiers" mapstructure:"IOSizeTiers"`
	EnsureDirMtime     bool               `json:"EnsureDirMtime" mapstructure:"EnsureDirMtime"`
	ProgressStorePath  string             `json:"ProgressStorePath" mapstructure:"ProgressStorePath"`
	ServerListenAddr   string             `json:"ServerListenAddr" mapstructure:"ServerListenAddr"`
	CksumAlgorithm     string             `json:"CksumAlgorithm" mapstructure:"CksumAlgorithm"`
	SkipByMtime        bool               `json:"SkipByMtime" mapstructure:"SkipByMtime"`
	ChannelBuf         int                `json:"ChannelBuf" mapstructure:"ChannelBuf"`
	OSSPartSize        int64              `json:"OSSPartSize" mapstructure:"OSSPartSize"`

	// Profiles holds named credential sets keyed by profile name.
	// Each profile is referenced from a URL via userinfo: oss://<profile>@bucket/path.
	Profiles map[string]model.Profile `json:"Profiles,omitempty" mapstructure:"Profiles"`

	DryRun bool   `json:"-" mapstructure:"-"`
	TaskID string `json:"-" mapstructure:"-"`
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{
		CopyParallelism:    2,
		ProgramLogLevel:    "warning",
		ProgramLogOutput:   "console",
		FileLogEnabled:     true,
		FileLogOutput:      "console",
		FileLogInterval:    5,
		DirectIO:           false,
		SyncWrites:         true,
		IOSize:             0,
		IOSizeTiers:        model.DefaultIOSizeTiers(),
		EnsureDirMtime:     true,
		ProgressStorePath:  "/tmp/ncp_progress_store",
		ServerListenAddr:   ":9900",
		CksumAlgorithm:     string(model.DefaultCksumAlgorithm),
		SkipByMtime:        true,
		OSSPartSize: 100 << 20, // 100MB
	}
}

// Validate checks config consistency.
func (c *Config) Validate() error {
	if c.DirectIO && c.SyncWrites {
		return fmt.Errorf("DirectIO and SyncWrites are mutually exclusive")
	}
	if c.CopyParallelism < 1 {
		return fmt.Errorf("CopyParallelism must be >= 1, got %d", c.CopyParallelism)
	}
	const minOSSPartSize = 5 << 20  // 5MB — cloud multipart minimum
	const maxOSSPartSize = 5 << 30  // 5GB — cloud multipart maximum
	if c.OSSPartSize > 0 && c.OSSPartSize < minOSSPartSize {
		return fmt.Errorf("OSSPartSize must be >= 5MB, got %d", c.OSSPartSize)
	}
	if c.OSSPartSize > maxOSSPartSize {
		return fmt.Errorf("OSSPartSize must be <= 5GB, got %d", c.OSSPartSize)
	}
	for name, p := range c.Profiles {
		if p.Provider == "" {
			return fmt.Errorf("profile %q: Provider is required", name)
		}
		if !model.IsCloudScheme(p.Provider) {
			return fmt.Errorf("profile %q: provider %q is not a recognized cloud scheme", name, p.Provider)
		}
	}
	return nil
}

// ConfigPaths returns the layered config file paths in priority order (low→high).
func ConfigPaths() []string {
	home, _ := os.UserHomeDir()
	return []string{
		"/etc/ncp_config.json",
		filepath.Join(home, "ncp_config.json"),
		"./ncp_config.json",
	}
}
