//go:build integration

package integration

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// ncpBinaryOnce builds the ncp CLI binary once per test run and caches the path.
var (
	ncpBuildOnce sync.Once
	ncpBuildPath string
	ncpBuildErr  error
)

func ncpBinary(t *testing.T) string {
	t.Helper()
	ncpBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "ncp-bin-*")
		if err != nil {
			ncpBuildErr = err
			return
		}
		bin := filepath.Join(dir, "ncp")
		// Build from the module root: go build resolves the package via the Go module path.
		cmd := exec.Command("go", "build", "-o", bin, "github.com/zp001/ncp/cmd/ncp")
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			ncpBuildErr = err
			return
		}
		ncpBuildPath = bin
	})
	if ncpBuildErr != nil {
		t.Fatalf("build ncp binary: %v", ncpBuildErr)
	}
	return ncpBuildPath
}

// withConfigHome creates an isolated $HOME with the given ncp_config.json content
// and returns the directory. Caller can pass it via `HOME=<dir>` to ncp.
func withConfigHome(t *testing.T, cfgJSON string) string {
	t.Helper()
	home := t.TempDir()
	if err := os.WriteFile(filepath.Join(home, "ncp_config.json"), []byte(cfgJSON), 0o600); err != nil {
		t.Fatalf("write home cfg: %v", err)
	}
	return home
}

func runNCP(t *testing.T, home string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(ncpBinary(t), args...)
	// Filter HOME out of the parent environment, then set our own so the
	// layered config resolver picks up tmpHome's ncp_config.json deterministically.
	env := []string{}
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "HOME=") {
			continue
		}
		env = append(env, kv)
	}
	env = append(env, "HOME="+home)
	cmd.Env = env
	cmd.Dir = t.TempDir() // isolate cwd so no stray ./ncp_config.json bleeds in
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// TestProfile_OSSCopyWithoutProfile_FailsFast verifies the CLI rejects an OSS URL
// that doesn't reference a profile.
func TestProfile_OSSCopyWithoutProfile_FailsFast(t *testing.T) {
	home := withConfigHome(t, `{"Profiles":{}}`)
	progressDir := filepath.Join(t.TempDir(), "progress")
	srcDir := filepath.Join(t.TempDir(), "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}

	out, err := runNCP(t, home,
		"copy", srcDir, "oss://bucket/path/",
		"--ProgressStorePath", progressDir,
	)
	if err == nil {
		t.Fatalf("expected non-zero exit for missing profile, output:\n%s", out)
	}
	if !strings.Contains(out, "requires a profile") {
		t.Fatalf("expected 'requires a profile' in output, got:\n%s", out)
	}
}

// TestProfile_UndefinedProfile_FailsFast verifies a URL referencing an unknown
// profile name produces a clear error.
func TestProfile_UndefinedProfile_FailsFast(t *testing.T) {
	home := withConfigHome(t, `{"Profiles":{}}`)
	progressDir := filepath.Join(t.TempDir(), "progress")
	srcDir := filepath.Join(t.TempDir(), "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}

	out, err := runNCP(t, home,
		"copy", srcDir, "oss://nosuch@bucket/path/",
		"--ProgressStorePath", progressDir,
	)
	if err == nil {
		t.Fatalf("expected non-zero exit, output:\n%s", out)
	}
	if !strings.Contains(out, "not defined") {
		t.Fatalf("expected 'not defined' in output, got:\n%s", out)
	}
}

// TestProfile_LocalCopyUnaffected verifies local→local copy still works without
// any profile configured.
func TestProfile_LocalCopyUnaffected(t *testing.T) {
	home := withConfigHome(t, `{}`)
	progressDir := filepath.Join(t.TempDir(), "progress")
	srcDir := filepath.Join(t.TempDir(), "src")
	dstDir := filepath.Join(t.TempDir(), "dst")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "hello.txt"), []byte("world"), 0o644); err != nil {
		t.Fatal(err)
	}

	out, err := runNCP(t, home,
		"copy", srcDir, dstDir,
		"--ProgressStorePath", progressDir,
		"--FileLogOutput", filepath.Join(t.TempDir(), "filelog.json"),
	)
	if err != nil {
		t.Fatalf("local copy failed: %v\noutput:\n%s", err, out)
	}
	got, err := os.ReadFile(filepath.Join(dstDir, filepath.Base(srcDir), "hello.txt"))
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if string(got) != "world" {
		t.Fatalf("dst content mismatch: got %q want %q", got, "world")
	}
}

// TestProfile_CrossAccountOSSCopy verifies that two distinct profiles can be
// referenced by src and dst URLs in the same command. We use the same OSS
// credentials twice under different names to avoid needing two real accounts;
// the point is that the resolver wires each URL to its own profile entry.
func TestProfile_CrossAccountOSSCopy(t *testing.T) {
	env := requireOSS(t)

	srcPrefix := newOSSPrefix(t, env, "profile-src")
	dstPrefix := newOSSPrefix(t, env, "profile-dst")

	files := map[string]string{
		"a.txt":     "alpha",
		"sub/b.txt": "beta",
	}
	seedOSSPrefix(t, env, srcPrefix, files)

	cfg := fmt.Sprintf(`{
		"Profiles": {
			"acct-src": {"Provider":"oss","Endpoint":%q,"Region":%q,"AK":%q,"SK":%q},
			"acct-dst": {"Provider":"oss","Endpoint":%q,"Region":%q,"AK":%q,"SK":%q}
		}
	}`,
		env.Endpoint, env.Region, env.AK, env.SK,
		env.Endpoint, env.Region, env.AK, env.SK,
	)
	home := withConfigHome(t, cfg)

	srcURL := fmt.Sprintf("oss://acct-src@%s/%s", env.Bucket, strings.TrimSuffix(srcPrefix, "/"))
	dstURL := fmt.Sprintf("oss://acct-dst@%s/%s", env.Bucket, strings.TrimSuffix(dstPrefix, "/"))

	progressDir := filepath.Join(t.TempDir(), "progress")
	out, err := runNCP(t, home,
		"copy", srcURL, dstURL,
		"--ProgressStorePath", progressDir,
		"--FileLogOutput", filepath.Join(t.TempDir(), "filelog.json"),
		"--cksum-algorithm", "md5",
	)
	if err != nil {
		t.Fatalf("cross-account copy failed: %v\noutput:\n%s", err, out)
	}

	// ncp copy places src under <basename> in the destination.
	srcBase := filepath.Base(strings.TrimSuffix(srcPrefix, "/"))
	expected := map[string]string{}
	for rel, content := range files {
		expected[strings.TrimSuffix(dstPrefix, "/")+"/"+srcBase+"/"+rel] = content
	}

	// Allow a moment for OSS to settle.
	time.Sleep(500 * time.Millisecond)

	for absKey, want := range expected {
		// The verify helper expects keys relative to a prefix; here the keys are
		// already absolute under the bucket. Build a tiny prefix-rooted lookup:
		key := absKey
		if !strings.HasPrefix(key, dstPrefix) {
			t.Fatalf("internal: expected key %q to start with %q", key, dstPrefix)
		}
		rel := strings.TrimPrefix(key, dstPrefix)
		verifyOSSPrefix(t, env, dstPrefix, map[string]string{rel: want})
	}
}
