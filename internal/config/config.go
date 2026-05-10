package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/zp001/ncp/pkg/model"
)

// Config holds all ncp configuration.
type Config struct {
	CopyParallelism   int                `json:"CopyParallelism" mapstructure:"CopyParallelism"`
	ProgramLogLevel   string             `json:"ProgramLogLevel" mapstructure:"ProgramLogLevel"`
	ProgramLogOutput  string             `json:"ProgramLogOutput" mapstructure:"ProgramLogOutput"`
	FileLogEnabled    bool               `json:"FileLogEnabled" mapstructure:"FileLogEnabled"`
	FileLogOutput     string             `json:"FileLogOutput" mapstructure:"FileLogOutput"`
	FileLogInterval   int                `json:"FileLogInterval" mapstructure:"FileLogInterval"`
	DirectIO          bool               `json:"DirectIO" mapstructure:"DirectIO"`
	SyncWrites        bool               `json:"SyncWrites" mapstructure:"SyncWrites"`
	IOSize            int                `json:"IOSize" mapstructure:"IOSize"`
	IOSizeTiers       []model.IOSizeTier `json:"IOSizeTiers" mapstructure:"IOSizeTiers"`
	EnsureDirMtime    bool               `json:"EnsureDirMtime" mapstructure:"EnsureDirMtime"`
	ProgressStorePath string             `json:"ProgressStorePath" mapstructure:"ProgressStorePath"`
	ServerListenAddr  string             `json:"ServerListenAddr" mapstructure:"ServerListenAddr"`
	CksumAlgorithm    string             `json:"CksumAlgorithm" mapstructure:"CksumAlgorithm"`
	SkipByMtime       bool               `json:"SkipByMtime" mapstructure:"SkipByMtime"`
	ChannelBuf        int                `json:"ChannelBuf" mapstructure:"ChannelBuf"`

	// Profiles holds named credential sets keyed by profile name.
	// Each profile is referenced from a URL via userinfo: oss://<profile>@bucket/path.
	Profiles map[string]model.Profile `json:"Profiles,omitempty" mapstructure:"Profiles"`

	DryRun bool   `json:"-" mapstructure:"-"`
	TaskID string `json:"-" mapstructure:"-"`
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{
		CopyParallelism:   1,
		ProgramLogLevel:   "warning",
		ProgramLogOutput:  "console",
		FileLogEnabled:    true,
		FileLogOutput:     "/tmp/ncp_file_log.json",
		FileLogInterval:   5,
		DirectIO:          false,
		SyncWrites:        true,
		IOSize:            0,
		IOSizeTiers:       model.DefaultIOSizeTiers(),
		EnsureDirMtime:    true,
		ProgressStorePath: "/tmp/ncp_progress_store",
		ServerListenAddr:  ":9900",
		CksumAlgorithm:    string(model.DefaultCksumAlgorithm),
		SkipByMtime:       true,
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
