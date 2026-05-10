package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"

	"github.com/zp001/ncp/pkg/model"
)

// Load builds a Config from layered sources:
// hardcoded defaults → /etc/ncp_config.json → ~/ncp_config.json → ./ncp_config.json → CLI flags
func Load() (*Config, error) {
	v := viper.New()
	setDefaults(v)
	profiles := loadProfilesLayered(v)

	v.SetEnvPrefix("NCP")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	cfg.Profiles = expandProfileEnv(profiles)

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// LoadFromViper builds Config from an existing Viper instance (after CLI flag binding).
func LoadFromViper(v *viper.Viper) (*Config, error) {
	setDefaults(v)
	profiles := loadProfilesLayered(v)

	v.SetEnvPrefix("NCP")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	cfg.Profiles = expandProfileEnv(profiles)

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
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
}

// loadProfilesLayered reads each config file once and merges the Profiles
// map by whole-profile replacement (not field-level merge), so layered files
// can never produce a half-old/half-new credential set. Other top-level
// fields are still merged through Viper.
func loadProfilesLayered(v *viper.Viper) map[string]model.Profile {
	v.SetConfigName("ncp_config")
	v.SetConfigType("json")

	merged := map[string]model.Profile{}
	for _, path := range ConfigPaths() {
		v.SetConfigFile(path)
		if err := v.MergeInConfig(); err != nil {
			continue // file may not exist; ignore
		}
		// Read this layer's profiles using a one-shot Viper instance to avoid
		// the deep-merge semantics of MergeInConfig contaminating the map.
		layer := viper.New()
		layer.SetConfigFile(path)
		layer.SetConfigType("json")
		if err := layer.ReadInConfig(); err != nil {
			continue
		}
		var layerProfiles map[string]model.Profile
		if err := layer.UnmarshalKey("Profiles", &layerProfiles); err != nil {
			continue
		}
		for name, p := range layerProfiles {
			merged[name] = p
		}
	}
	return merged
}

func expandProfileEnv(profiles map[string]model.Profile) map[string]model.Profile {
	if profiles == nil {
		return nil
	}
	out := make(map[string]model.Profile, len(profiles))
	for name, p := range profiles {
		p.ExpandEnv()
		out[name] = p
	}
	return out
}
