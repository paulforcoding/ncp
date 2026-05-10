package model

import (
	"fmt"
	"os"
	"strings"
)

// Profile describes credentials for one cloud endpoint.
// Field names AK/SK are unified across providers; backends map them to
// SDK-specific names (e.g. COS uses SecretId/SecretKey internally).
type Profile struct {
	Provider string `json:"Provider"           mapstructure:"Provider"`
	Endpoint string `json:"Endpoint,omitempty" mapstructure:"Endpoint"`
	Region   string `json:"Region,omitempty"   mapstructure:"Region"`
	AK       string `json:"AK,omitempty"       mapstructure:"AK"`
	SK       string `json:"SK,omitempty"       mapstructure:"SK"`
}

// CloudSchemes lists URL schemes that require a profile.
// Add new backends here when their support lands.
var CloudSchemes = map[string]struct{}{
	"oss": {},
	// "cos": {},  // P3
	// "obs": {},
	// "s3":  {},
}

// IsCloudScheme reports whether the scheme requires a profile reference.
func IsCloudScheme(scheme string) bool {
	_, ok := CloudSchemes[scheme]
	return ok
}

// ExpandEnv expands ${env:VAR} placeholders in AK/SK/Endpoint/Region.
// Only an exact match of the form "${env:NAME}" is recognized; any other
// shape (partial inclusion, malformed) is left unchanged to avoid surprises.
func (p *Profile) ExpandEnv() {
	p.AK = expandEnvPlaceholder(p.AK)
	p.SK = expandEnvPlaceholder(p.SK)
	p.Endpoint = expandEnvPlaceholder(p.Endpoint)
	p.Region = expandEnvPlaceholder(p.Region)
}

func expandEnvPlaceholder(s string) string {
	if !strings.HasPrefix(s, "${env:") || !strings.HasSuffix(s, "}") {
		return s
	}
	name := strings.TrimSuffix(strings.TrimPrefix(s, "${env:"), "}")
	if name == "" {
		return s
	}
	return os.Getenv(name)
}

// Validate checks provider/scheme consistency and required fields.
// name is the profile's key in the map; used for error messages.
func (p Profile) Validate(name, scheme string) error {
	if p.Provider == "" {
		return fmt.Errorf("profile %q: Provider is required", name)
	}
	if p.Provider != scheme {
		return fmt.Errorf("profile %q: Provider=%q does not match URL scheme=%q",
			name, p.Provider, scheme)
	}
	switch p.Provider {
	case "oss":
		if p.AK == "" || p.SK == "" || p.Endpoint == "" || p.Region == "" {
			return fmt.Errorf("profile %q (oss): AK/SK/Endpoint/Region are all required", name)
		}
	default:
		return fmt.Errorf("profile %q: unknown provider %q", name, p.Provider)
	}
	return nil
}

// HasPlainSecret reports whether AK or SK contains a plain-text value
// (i.e. not a "${env:...}" reference). Used by config permission checks.
func (p Profile) HasPlainSecret() bool {
	return (p.AK != "" && !isEnvRef(p.AK)) || (p.SK != "" && !isEnvRef(p.SK))
}

func isEnvRef(s string) bool {
	return strings.HasPrefix(s, "${env:") && strings.HasSuffix(s, "}")
}
