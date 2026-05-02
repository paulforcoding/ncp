package config

import (
	"fmt"
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
		fmt.Fprintf(os.Stderr, "{\"warn\":\"config file %s has overly permissive mode %04o, recommend 0600\"}\n", path, perm)
	}
}
