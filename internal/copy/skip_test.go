package copy

import (
	"testing"

	"github.com/zp001/ncp/pkg/model"
)

func TestMatchSkip(t *testing.T) {
	tests := []struct {
		name string
		src  model.DiscoverItem
		dst  model.DiscoverItem
		want bool
	}{
		{
			"dir always matches",
			model.DiscoverItem{FileType: model.FileDir},
			model.DiscoverItem{FileType: model.FileDir},
			true,
		},
		{
			"dir vs file mismatch",
			model.DiscoverItem{FileType: model.FileDir},
			model.DiscoverItem{FileType: model.FileRegular},
			false,
		},
		{
			"symlink same target",
			model.DiscoverItem{FileType: model.FileSymlink, LinkTarget: "a.txt"},
			model.DiscoverItem{FileType: model.FileSymlink, LinkTarget: "a.txt"},
			true,
		},
		{
			"symlink different target",
			model.DiscoverItem{FileType: model.FileSymlink, LinkTarget: "a.txt"},
			model.DiscoverItem{FileType: model.FileSymlink, LinkTarget: "b.txt"},
			false,
		},
		{
			"regular same mtime+size",
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 100, Mtime: 1700000000},
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 100, Mtime: 1700000000},
			true,
		},
		{
			"regular different size",
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 100, Mtime: 1700000000},
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 200, Mtime: 1700000000},
			false,
		},
		{
			"regular different mtime",
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 100, Mtime: 1700000000},
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 100, Mtime: 1700000001},
			false,
		},
		{
			"regular same etag",
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 100, ETag: "abc"},
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 100, ETag: "abc"},
			true,
		},
		{
			"regular different etag",
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 100, ETag: "abc"},
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 100, ETag: "def"},
			false,
		},
		{
			"regular etag beats mtime",
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 100, Mtime: 1700000000, ETag: "abc"},
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 100, Mtime: 1700000000, ETag: "abc"},
			true,
		},
		{
			"regular etag mismatch even if mtime matches",
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 100, Mtime: 1700000000, ETag: "abc"},
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 100, Mtime: 1700000000, ETag: "def"},
			false,
		},
		{
			"regular zero mtime no etag",
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 100, Mtime: 0},
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 100, Mtime: 0},
			false,
		},
		{
			"regular one side etag other side mtime",
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 100, ETag: "abc"},
			model.DiscoverItem{FileType: model.FileRegular, FileSize: 100, Mtime: 1700000000},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MatchSkip(tt.src, tt.dst)
			if got != tt.want {
				t.Errorf("MatchSkip() = %v, want %v", got, tt.want)
			}
		})
	}
}
