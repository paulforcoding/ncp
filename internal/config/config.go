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

	// OSS configuration
	OSSEndpoint string `json:"OSSEndpoint,omitempty" mapstructure:"OSSEndpoint"`
	OSSRegion   string `json:"OSSRegion,omitempty" mapstructure:"OSSRegion"`
	OSSAK       string `json:"-" mapstructure:"OSSAK"`
	OSSSK       string `json:"-" mapstructure:"OSSSK"`

	DryRun bool `json:"-" mapstructure:"-"`
	TaskID string `json:"-" mapstructure:"-"`
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{
		CopyParallelism:   1,
		ProgramLogLevel:   "info",
		ProgramLogOutput:  "console",
		FileLogEnabled:    true,
		FileLogOutput:     "console",
		FileLogInterval:   5,
		DirectIO:          false,
		SyncWrites:        true,
		IOSize:            0,
		IOSizeTiers:       model.DefaultIOSizeTiers(),
		EnsureDirMtime:    true,
		ProgressStorePath: "./progress",
		ServerListenAddr:  ":9900",
		CksumAlgorithm:    string(model.DefaultCksumAlgorithm),
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
