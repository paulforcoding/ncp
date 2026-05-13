package mkfiles

import (
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	insecurerand "math/rand"
	"os"
	"path/filepath"
)

type Config struct {
	Dir         string
	NumFiles    int
	MinSize     int64
	MaxSize     int64
	MaxDirDepth int
}

type Generator struct {
	cfg  Config
	dirs []string
	rng  *insecurerand.Rand
}

func NewGenerator(cfg Config) (*Generator, error) {
	if cfg.NumFiles <= 0 {
		return nil, fmt.Errorf("num must be > 0, got %d", cfg.NumFiles)
	}
	if cfg.MinSize < 0 {
		return nil, fmt.Errorf("minsize must be >= 0, got %d", cfg.MinSize)
	}
	if cfg.MaxSize < cfg.MinSize {
		return nil, fmt.Errorf("maxsize must be >= minsize, got maxsize=%d minsize=%d", cfg.MaxSize, cfg.MinSize)
	}
	if cfg.MaxDirDepth < 0 {
		return nil, fmt.Errorf("maxdirdepth must be >= 0, got %d", cfg.MaxDirDepth)
	}
	seed, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		return nil, fmt.Errorf("generate seed: %w", err)
	}
	return &Generator{
		cfg: cfg,
		rng: insecurerand.New(insecurerand.NewSource(seed.Int64())),
	}, nil
}

func (g *Generator) Run() error {
	if err := g.buildDirs(); err != nil {
		return err
	}
	return g.createFiles()
}

func (g *Generator) buildDirs() error {
	g.dirs = []string{g.cfg.Dir}
	if g.cfg.MaxDirDepth == 0 {
		return os.MkdirAll(g.cfg.Dir, 0o755)
	}
	if err := os.MkdirAll(g.cfg.Dir, 0o755); err != nil {
		return fmt.Errorf("create root dir: %w", err)
	}
	for depth := 1; depth <= g.cfg.MaxDirDepth; depth++ {
		count := 1 << (depth - 1)
		parentStart := 0
		if depth > 1 {
			parentStart = (1 << (depth - 2)) - 1
		} else {
			parentStart = 0
		}
		parentCount := 1
		if depth > 1 {
			parentCount = 1 << (depth - 2)
		}
		// parentCount directories at this parent level, each gets count/parentCount children
		childrenPerParent := count / parentCount
		for p := 0; p < parentCount; p++ {
			parentIdx := parentStart + p
			parent := g.dirs[parentIdx]
			for c := 0; c < childrenPerParent; c++ {
				name := randName(g.rng, 8)
				child := filepath.Join(parent, name)
				if err := os.MkdirAll(child, 0o755); err != nil {
					return fmt.Errorf("create dir %s: %w", child, err)
				}
				g.dirs = append(g.dirs, child)
			}
		}
	}
	return nil
}

func (g *Generator) createFiles() error {
	buf := make([]byte, 32*1024)
	for i := 0; i < g.cfg.NumFiles; i++ {
		dir := g.dirs[g.rng.Intn(len(g.dirs))]
		name := randName(g.rng, 12)
		path := filepath.Join(dir, name)

		size := g.cfg.MinSize
		if g.cfg.MaxSize > g.cfg.MinSize {
			size += g.rng.Int63n(g.cfg.MaxSize - g.cfg.MinSize + 1)
		}

		if err := g.writeFile(path, size, buf); err != nil {
			return fmt.Errorf("write file %s: %w", path, err)
		}
	}
	return nil
}

func (g *Generator) writeFile(path string, size int64, buf []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var written int64
	for written < size {
		remaining := size - written
		n := len(buf)
		if remaining < int64(n) {
			n = int(remaining)
		}
		if _, err := rand.Read(buf[:n]); err != nil {
			return fmt.Errorf("generate random data: %w", err)
		}
		got, err := f.Write(buf[:n])
		if err != nil {
			return err
		}
		written += int64(got)
	}
	return nil
}

const nameChars = "abcdefghijklmnopqrstuvwxyz0123456789"

func randName(rng *insecurerand.Rand, length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = nameChars[rng.Intn(len(nameChars))]
	}
	return string(b)
}
