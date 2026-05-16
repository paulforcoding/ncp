package di

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// mockSource is a test-only Source implementation.
type mockSource struct {
	base  string
	items []storage.DiscoverItem
}

func (m *mockSource) Walk(ctx context.Context, fn func(context.Context, storage.DiscoverItem) error) error {
	for _, item := range m.items {
		if err := fn(ctx, item); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockSource) Open(ctx context.Context, relPath string) (storage.FileReader, error) {
	return nil, fmt.Errorf("mock open: %s", relPath)
}

func (m *mockSource) Stat(_ context.Context, relPath string) (storage.DiscoverItem, error) {
	return storage.DiscoverItem{RelPath: relPath}, nil
}

func (m *mockSource) URI() string { return m.base }

func (m *mockSource) Connect(ctx context.Context) error                              { return nil }
func (m *mockSource) Close(ctx context.Context) error                                { return nil }
func (m *mockSource) BeginTask(ctx context.Context, taskID string) error             { return nil }
func (m *mockSource) EndTask(ctx context.Context, summary storage.TaskSummary) error { return nil }

func TestBasenamePrefixedSource_SingleDir(t *testing.T) {
	inner := &mockSource{
		base: "/data/mydir",
		items: []storage.DiscoverItem{
			{RelPath: "a.txt", FileType: model.FileRegular},
			{RelPath: "sub/b.txt", FileType: model.FileRegular},
		},
	}

	bps, err := NewBasenamePrefixedSource([]storage.Source{inner}, []string{"mydir"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	var items []storage.DiscoverItem
	if err := bps.Walk(context.Background(), func(ctx context.Context, item storage.DiscoverItem) error {
		items = append(items, item)
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}

	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if items[0].RelPath != "mydir/a.txt" {
		t.Fatalf("item0 relPath: got %q, want mydir/a.txt", items[0].RelPath)
	}
	if items[1].RelPath != "mydir/sub/b.txt" {
		t.Fatalf("item1 relPath: got %q, want mydir/sub/b.txt", items[1].RelPath)
	}
}

func TestBasenamePrefixedSource_MultiSource(t *testing.T) {
	srcA := &mockSource{base: "/data/a", items: []storage.DiscoverItem{{RelPath: "f1.txt", FileType: model.FileRegular}}}
	srcB := &mockSource{base: "/data/b", items: []storage.DiscoverItem{{RelPath: "f2.txt", FileType: model.FileRegular}}}

	bps, err := NewBasenamePrefixedSource([]storage.Source{srcA, srcB}, []string{"a", "b"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	var items []storage.DiscoverItem
	if err := bps.Walk(context.Background(), func(ctx context.Context, item storage.DiscoverItem) error {
		items = append(items, item)
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}

	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if items[0].RelPath != "a/f1.txt" {
		t.Fatalf("item0: got %q, want a/f1.txt", items[0].RelPath)
	}
	if items[1].RelPath != "b/f2.txt" {
		t.Fatalf("item1: got %q, want b/f2.txt", items[1].RelPath)
	}
}

func TestBasenamePrefixedSource_SingleFile(t *testing.T) {
	inner := &mockSource{
		base:  "/data/file.txt",
		items: []storage.DiscoverItem{{RelPath: "", FileType: model.FileRegular}},
	}

	bps, err := NewBasenamePrefixedSource([]storage.Source{inner}, []string{"file.txt"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	var items []storage.DiscoverItem
	if err := bps.Walk(context.Background(), func(ctx context.Context, item storage.DiscoverItem) error {
		items = append(items, item)
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}

	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].RelPath != "file.txt" {
		t.Fatalf("item relPath: got %q, want file.txt", items[0].RelPath)
	}
}

func TestBasenamePrefixedSource_RouteSingleFile(t *testing.T) {
	inner := &mockSource{
		base:  "/data/file.txt",
		items: []storage.DiscoverItem{{RelPath: "", FileType: model.FileRegular}},
	}

	bps, err := NewBasenamePrefixedSource([]storage.Source{inner}, []string{"file.txt"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	item, err := bps.Stat(context.Background(), "file.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if item.RelPath != "file.txt" {
		t.Fatalf("stat relPath: got %q, want file.txt", item.RelPath)
	}
}

func TestBasenamePrefixedSource_ZeroSources(t *testing.T) {
	_, err := NewBasenamePrefixedSource(nil, nil)
	if err == nil {
		t.Fatal("expected error for zero sources")
	}
}

func TestBasenamePrefixedSource_MismatchedCounts(t *testing.T) {
	inner := &mockSource{base: "/data/a", items: []storage.DiscoverItem{}}
	_, err := NewBasenamePrefixedSource([]storage.Source{inner}, []string{"a", "b"})
	if err == nil {
		t.Fatal("expected error for mismatched counts")
	}
}

func TestSourceBasename_LocalDir(t *testing.T) {
	u, _ := ParsePath("/data/mydir")
	inner := &mockSource{base: u.Path}
	got := SourceBasename(inner, "/data/mydir")
	if got != "mydir" {
		t.Fatalf("got %q, want mydir", got)
	}
}

func TestSourceBasename_LocalFile(t *testing.T) {
	u, _ := ParsePath("/data/file.txt")
	inner := &mockSource{base: u.Path}
	got := SourceBasename(inner, "/data/file.txt")
	if got != "file.txt" {
		t.Fatalf("got %q, want file.txt", got)
	}
}

func TestSourceBasename_OSSWithPrefix(t *testing.T) {
	inner := &mockSource{base: "oss://mybucket/photos/2024/"}
	got := SourceBasename(inner, "oss://mybucket/photos/2024/")
	if got != "2024" {
		t.Fatalf("got %q, want 2024", got)
	}
}

func TestSourceBasename_OSSWholeBucket(t *testing.T) {
	inner := &mockSource{base: "oss://mybucket/"}
	got := SourceBasename(inner, "oss://mybucket/")
	if got != "mybucket" {
		t.Fatalf("got %q, want mybucket", got)
	}
}

func TestBasenamePrefixedSource_OpenRoutesCorrectly(t *testing.T) {
	srcA := &mockSource{base: "/data/a", items: []storage.DiscoverItem{{RelPath: "f1.txt"}}}
	srcB := &mockSource{base: "/data/b", items: []storage.DiscoverItem{{RelPath: "f2.txt"}}}

	mockOpenA := &mockOpenSource{mockSource: srcA, openFunc: func(relPath string) (storage.FileReader, error) {
		return nil, fmt.Errorf("mock open: %s", relPath)
	}}
	bps, err := NewBasenamePrefixedSource([]storage.Source{mockOpenA, srcB}, []string{"a", "b"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	// Open "a/f1.txt" should route to srcA with stripped path "f1.txt"
	_, err = bps.Open(context.Background(), "a/f1.txt")
	if err == nil {
		t.Fatal("expected error from mock open")
	}
	if !strings.Contains(err.Error(), "f1.txt") {
		t.Errorf("expected stripped relPath 'f1.txt' in error, got: %v", err)
	}
}

func TestBasenamePrefixedSource_StatRoutesCorrectly(t *testing.T) {
	srcA := &mockSource{base: "/data/a", items: []storage.DiscoverItem{{RelPath: "f1.txt"}}}
	srcB := &mockSource{base: "/data/b", items: []storage.DiscoverItem{{RelPath: "f2.txt"}}}

	bps, err := NewBasenamePrefixedSource([]storage.Source{srcA, srcB}, []string{"a", "b"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	// Stat preserves the original relPath (with basename prefix)
	item, err := bps.Stat(context.Background(), "a/f1.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if item.RelPath != "a/f1.txt" {
		t.Fatalf("stat relPath: got %q, want a/f1.txt", item.RelPath)
	}
}

type mockOpenSource struct {
	*mockSource
	openFunc func(string) (storage.FileReader, error)
}

func (m *mockOpenSource) Open(ctx context.Context, relPath string) (storage.FileReader, error) {
	if m.openFunc != nil {
		return m.openFunc(relPath)
	}
	return nil, fmt.Errorf("mock open: %s", relPath)
}

func TestSourceBasename_OSSNoTrailingSlash(t *testing.T) {
	inner := &mockSource{base: "oss://mybucket"}
	got := SourceBasename(inner, "oss://mybucket")
	if got != "mybucket" {
		t.Fatalf("got %q, want mybucket", got)
	}
}
