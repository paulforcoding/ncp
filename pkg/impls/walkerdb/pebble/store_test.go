package pebble

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/zp001/ncp/internal/protocol"
)

func TestStoreOpenClose(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "walker")

	s := &Store{}
	if err := s.Open(dbDir); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		t.Fatal("db dir should exist after Close")
	}
}

func TestStorePutGetRange(t *testing.T) {
	dir := t.TempDir()
	s := &Store{}
	if err := s.Open(filepath.Join(dir, "walker")); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	entries := []protocol.ListEntry{
		{RelPath: "a.txt", FileType: 1, FileSize: 100},
		{RelPath: "b.txt", FileType: 1, FileSize: 200},
		{RelPath: "c.txt", FileType: 2, FileSize: 0},
	}

	for i, e := range entries {
		if err := s.Put(int64(i), e); err != nil {
			t.Fatalf("Put seq %d failed: %v", i, err)
		}
	}

	got, err := s.GetRange(0, 2)
	if err != nil {
		t.Fatalf("GetRange failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	if got[0].Seq != 0 || got[0].Entry.RelPath != "a.txt" {
		t.Fatalf("first entry mismatch: %+v", got[0])
	}
	if got[1].Seq != 1 || got[1].Entry.RelPath != "b.txt" {
		t.Fatalf("second entry mismatch: %+v", got[1])
	}

	// Get remaining
	got2, err := s.GetRange(2, 10)
	if err != nil {
		t.Fatalf("GetRange(2,10) failed: %v", err)
	}
	if len(got2) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got2))
	}
	if got2[0].Seq != 2 || got2[0].Entry.RelPath != "c.txt" {
		t.Fatalf("third entry mismatch: %+v", got2[0])
	}
}

func TestStoreWalkComplete(t *testing.T) {
	dir := t.TempDir()
	s := &Store{}
	if err := s.Open(filepath.Join(dir, "walker")); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer s.Close()

	done, err := s.IsWalkComplete()
	if err != nil {
		t.Fatalf("IsWalkComplete failed: %v", err)
	}
	if done {
		t.Fatal("expected walk not complete")
	}

	if err := s.SetWalkComplete(); err != nil {
		t.Fatalf("SetWalkComplete failed: %v", err)
	}

	done, err = s.IsWalkComplete()
	if err != nil {
		t.Fatalf("IsWalkComplete failed: %v", err)
	}
	if !done {
		t.Fatal("expected walk complete")
	}
}

func TestStoreDestroy(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, "walker")

	s := &Store{}
	if err := s.Open(dbDir); err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := s.Destroy(); err != nil {
		t.Fatalf("Destroy failed: %v", err)
	}
	if _, err := os.Stat(dbDir); !os.IsNotExist(err) {
		t.Fatal("db dir should be removed after Destroy")
	}
}
