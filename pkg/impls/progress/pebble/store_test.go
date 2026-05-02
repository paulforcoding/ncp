package pebble

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/zp001/ncp/pkg/model"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "pebble-test")
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func openStore(t *testing.T) *Store {
	t.Helper()
	s := &Store{}
	if err := s.Open(tempDir(t)); err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestEncodeDecodeValue(t *testing.T) {
	cs := model.CopyDone
	cks := model.CksumPass
	val := encodeValue(cs, cks)
	if len(val) != 2 {
		t.Fatalf("expected 2 bytes, got %d", len(val))
	}
	gotCS, gotCKS := decodeValue(val)
	if gotCS != cs || gotCKS != cks {
		t.Fatalf("expected (%d,%d), got (%d,%d)", cs, cks, gotCS, gotCKS)
	}
}

func TestEncodeDecodeValueEmpty(t *testing.T) {
	gotCS, gotCKS := decodeValue(nil)
	if gotCS != model.CopyDiscovered || gotCKS != model.CksumNone {
		t.Fatalf("expected defaults, got (%d,%d)", gotCS, gotCKS)
	}
}

func TestSetGet(t *testing.T) {
	s := openStore(t)

	if err := s.Set("dir/file.txt", model.CopyDispatched, model.CksumNone); err != nil {
		t.Fatalf("set: %v", err)
	}
	cs, cks, err := s.Get("dir/file.txt")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if cs != model.CopyDispatched || cks != model.CksumNone {
		t.Fatalf("expected (dispatched, none), got (%d, %d)", cs, cks)
	}
}

func TestGetNotFound(t *testing.T) {
	s := openStore(t)

	cs, cks, err := s.Get("nonexistent")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if cs != model.CopyDiscovered || cks != model.CksumNone {
		t.Fatalf("expected defaults for missing key, got (%d, %d)", cs, cks)
	}
}

func TestBatch(t *testing.T) {
	s := openStore(t)

	b := s.Batch()
	b.Set("a.txt", model.CopyDiscovered, model.CksumNone)
	b.Set("b.txt", model.CopyDispatched, model.CksumNone)
	if err := b.Commit(false); err != nil {
		t.Fatalf("batch commit: %v", err)
	}
	b.Close()

	cs, _, _ := s.Get("a.txt")
	if cs != model.CopyDiscovered {
		t.Fatalf("a.txt: expected discovered, got %d", cs)
	}
	cs, _, _ = s.Get("b.txt")
	if cs != model.CopyDispatched {
		t.Fatalf("b.txt: expected dispatched, got %d", cs)
	}
}

func TestIter(t *testing.T) {
	s := openStore(t)

	s.Set("a.txt", model.CopyDone, model.CksumPass)
	s.Set("b.txt", model.CopyError, model.CksumNone)

	it, err := s.Iter()
	if err != nil {
		t.Fatalf("iter: %v", err)
	}
	defer it.Close()

	var keys []string
	for it.First(); it.Valid(); it.Next() {
		keys = append(keys, it.Key())
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d: %v", len(keys), keys)
	}
	if keys[0] != "a.txt" || keys[1] != "b.txt" {
		t.Fatalf("unexpected key order: %v", keys)
	}
}

func TestDelete(t *testing.T) {
	s := openStore(t)

	s.Set("del.txt", model.CopyDone, model.CksumNone)
	if err := s.Delete("del.txt"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	cs, _, _ := s.Get("del.txt")
	if cs != model.CopyDiscovered {
		t.Fatalf("expected default after delete, got %d", cs)
	}
}

func TestWalkComplete(t *testing.T) {
	s := openStore(t)

	ok, err := s.HasWalkComplete()
	if err != nil {
		t.Fatalf("has walk complete: %v", err)
	}
	if ok {
		t.Fatal("expected no walk_complete initially")
	}

	if err := s.SetWalkComplete(42); err != nil {
		t.Fatalf("set walk complete: %v", err)
	}

	ok, err = s.HasWalkComplete()
	if err != nil {
		t.Fatalf("has walk complete: %v", err)
	}
	if !ok {
		t.Fatal("expected walk_complete after set")
	}
}

func TestDestroy(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble-destroy")
	s := &Store{}
	if err := s.Open(dir); err != nil {
		t.Fatalf("open: %v", err)
	}
	s.Set("x.txt", model.CopyDone, model.CksumNone)

	if err := s.Destroy(); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("expected dir to be removed after destroy")
	}
}
