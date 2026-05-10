package config

import (
	"errors"
	"fmt"
	"os"

	"github.com/zp001/ncp/pkg/model"
)

// CheckCredentialFilePermissions verifies that any config file potentially
// holding plain-text credentials has mode 0600 or stricter.
//
// If at least one profile in `profiles` carries a plain-text AK or SK (i.e.
// not a "${env:...}" reference), every existing config file in the layered
// search path is checked. Files with broader modes produce an error.
//
// If `profiles` is empty or every profile relies on environment-variable
// indirection, this function returns nil (no plain credentials at risk).
func CheckCredentialFilePermissions(profiles map[string]model.Profile) error {
	if !anyPlainSecret(profiles) {
		return nil
	}

	var errs []error
	for _, path := range ConfigPaths() {
		if err := checkFilePerms(path); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func anyPlainSecret(profiles map[string]model.Profile) bool {
	for _, p := range profiles {
		if p.HasPlainSecret() {
			return true
		}
	}
	return false
}

func checkFilePerms(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return nil // file doesn't exist — nothing to check
	}
	perm := info.Mode().Perm()
	if perm&0o077 != 0 {
		return fmt.Errorf("config file %s has overly permissive mode %04o; must be 0600 when storing plain credentials", path, perm)
	}
	return nil
}
