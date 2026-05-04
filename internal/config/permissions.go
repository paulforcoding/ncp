package config

import (
	"fmt"
	"log/slog"
	"os"
)

// CheckCredentialFilePermissions warns if config files containing
// credentials have permissions broader than 0600 (NFR10).
func CheckCredentialFilePermissions() {
	for _, path := range ConfigPaths() {
		checkFilePerms(path)
	}
}

func checkFilePerms(path string) {
	info, err := os.Stat(path)
	if err != nil {
		return // file doesn't exist, nothing to check
	}

	perm := info.Mode().Perm()
	if perm&0077 != 0 {
		slog.Warn("config file has overly permissive mode", "path", path, "mode", fmt.Sprintf("%04o", perm))
	}
}
