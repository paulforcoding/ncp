package mkfiles

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewGeneratorValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{"zero num", Config{Dir: t.TempDir(), NumFiles: 0}, "num must be > 0"},
		{"negative minsize", Config{Dir: t.TempDir(), NumFiles: 1, MinSize: -1}, "minsize must be >= 0"},
		{"maxsize < minsize", Config{Dir: t.TempDir(), NumFiles: 1, MinSize: 100, MaxSize: 50}, "maxsize must be >= minsize"},
		{"negative maxdirdepth", Config{Dir: t.TempDir(), NumFiles: 1, MaxDirDepth: -1}, "maxdirdepth must be >= 0"},
		{"valid", Config{Dir: t.TempDir(), NumFiles: 1, MinSize: 0, MaxSize: 100, MaxDirDepth: 0}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewGenerator(tt.cfg)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
		})
	}
}

func TestFlatGeneration(t *testing.T) {
	dir := t.TempDir()
	gen, err := NewGenerator(Config{
		Dir:         dir,
		NumFiles:    10,
		MinSize:     100,
		MaxSize:     500,
		MaxDirDepth: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := gen.Run(); err != nil {
		t.Fatal(err)
	}

	files, _ := filepath.Glob(filepath.Join(dir, "*"))
	if len(files) != 10 {
		t.Fatalf("expected 10 files, got %d", len(files))
	}

	// All entries should be files (no subdirectories)
	for _, f := range files {
		info, err := os.Stat(f)
		if err != nil {
			t.Fatal(err)
		}
		if info.IsDir() {
			t.Fatalf("expected file, got directory: %s", f)
		}
		if info.Size() < 100 || info.Size() > 500 {
			t.Fatalf("file size %d outside range [100, 500]", info.Size())
		}
	}
}

func TestNestedDirectoryStructure(t *testing.T) {
	dir := t.TempDir()
	depth := 4
	gen, err := NewGenerator(Config{
		Dir:         dir,
		NumFiles:    50,
		MinSize:     10,
		MaxSize:     100,
		MaxDirDepth: depth,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := gen.Run(); err != nil {
		t.Fatal(err)
	}

	// depth=4: 1+2+4+8=15 subdirectories + 1 root = 16 total dirs
	expectedDirs := 1
	for d := 1; d <= depth; d++ {
		expectedDirs += 1 << (d - 1)
	}

	dirCount := 0
	fileCount := 0
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			dirCount++
		} else {
			fileCount++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if dirCount != expectedDirs {
		t.Fatalf("expected %d directories, got %d", expectedDirs, dirCount)
	}
	if fileCount != 50 {
		t.Fatalf("expected 50 files, got %d", fileCount)
	}
}

func TestMinSizeEqualsMaxSize(t *testing.T) {
	dir := t.TempDir()
	gen, err := NewGenerator(Config{
		Dir:      dir,
		NumFiles: 5,
		MinSize:  256,
		MaxSize:  256,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := gen.Run(); err != nil {
		t.Fatal(err)
	}

	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != 256 {
			t.Fatalf("expected size 256, got %d", info.Size())
		}
	}
}

func TestFilesDistributedAcrossDirs(t *testing.T) {
	dir := t.TempDir()
	gen, err := NewGenerator(Config{
		Dir:         dir,
		NumFiles:    100,
		MinSize:     1,
		MaxSize:     10,
		MaxDirDepth: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := gen.Run(); err != nil {
		t.Fatal(err)
	}

	// depth=2: root + 1 + 2 = 4 dirs, files should be in multiple dirs
	dirsWithFiles := 0
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			entries, _ := os.ReadDir(path)
			hasFile := false
			for _, e := range entries {
				if !e.IsDir() {
					hasFile = true
					break
				}
			}
			if hasFile {
				dirsWithFiles++
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if dirsWithFiles < 2 {
		t.Fatalf("expected files in at least 2 directories, got %d", dirsWithFiles)
	}
}
