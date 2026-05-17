package ncpserver

import (
	"os"
	"strings"
)

// CleanupTempDir removes stale walker DB directories from previous server runs.
// Uses the provided tempDir path (sourced from ServerConfig.ProgressStorePath).
func CleanupTempDir(tempDir string) error {
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "walker-") {
			_ = os.RemoveAll(tempDir + "/" + entry.Name())
		}
	}
	return nil
}
