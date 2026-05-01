package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Load builds a Config from layered sources:
// hardcoded defaults → /etc/ncp_config.json → ~/ncp_config.json → ./ncp_config.json → CLI flags
func Load() (*Config, error) {
	v := viper.New()

	// Set hardcoded defaults
	def := DefaultConfig()
	v.SetDefault("CopyParallelism", def.CopyParallelism)
	v.SetDefault("ProgramLogLevel", def.ProgramLogLevel)
	v.SetDefault("ProgramLogOutput", def.ProgramLogOutput)
	v.SetDefault("FileLogEnabled", def.FileLogEnabled)
	v.SetDefault("FileLogOutput", def.FileLogOutput)
	v.SetDefault("FileLogInterval", def.FileLogInterval)
	v.SetDefault("DirectIO", def.DirectIO)
	v.SetDefault("SyncWrites", def.SyncWrites)
	v.SetDefault("IOSize", def.IOSize)
	v.SetDefault("EnsureDirMtime", def.EnsureDirMtime)
	v.SetDefault("ProgressStorePath", def.ProgressStorePath)
	v.SetDefault("ServerListenAddr", def.ServerListenAddr)

	// Read layered config files (low priority first)
	v.SetConfigName("ncp_config")
	v.SetConfigType("json")

	for _, path := range ConfigPaths() {
		v.SetConfigFile(path)
		_ = v.MergeInConfig() // ignore error: file may not exist
	}

	// Environment variable overrides: NCP_<FIELD>
	v.SetEnvPrefix("NCP")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// CLI flags are bound by the caller (cmd/ncp/main.go) via v.BindPFlag

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// LoadFromViper builds Config from an existing Viper instance (after CLI flag binding).
func LoadFromViper(v *viper.Viper) (*Config, error) {
	// Set defaults
	def := DefaultConfig()
	v.SetDefault("CopyParallelism", def.CopyParallelism)
	v.SetDefault("ProgramLogLevel", def.ProgramLogLevel)
	v.SetDefault("ProgramLogOutput", def.ProgramLogOutput)
	v.SetDefault("FileLogEnabled", def.FileLogEnabled)
	v.SetDefault("FileLogOutput", def.FileLogOutput)
	v.SetDefault("FileLogInterval", def.FileLogInterval)
	v.SetDefault("DirectIO", def.DirectIO)
	v.SetDefault("SyncWrites", def.SyncWrites)
	v.SetDefault("IOSize", def.IOSize)
	v.SetDefault("EnsureDirMtime", def.EnsureDirMtime)
	v.SetDefault("ProgressStorePath", def.ProgressStorePath)
	v.SetDefault("ServerListenAddr", def.ServerListenAddr)

	// Read layered config files
	v.SetConfigName("ncp_config")
	v.SetConfigType("json")
	for _, path := range ConfigPaths() {
		v.SetConfigFile(path)
		_ = v.MergeInConfig()
	}

	v.SetEnvPrefix("NCP")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}
