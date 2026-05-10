package copy

import (
	"testing"
	"time"

	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

func TestMatchSkip(t *testing.T) {
	t1 := time.Unix(1700000000, 0)
	t2 := time.Unix(1700000001, 0)

	tests := []struct {
		name string
		src  storage.DiscoverItem
		dst  storage.DiscoverItem
		want bool
	}{
		{
			"dir always matches",
			storage.DiscoverItem{FileType: model.FileDir},
			storage.DiscoverItem{FileType: model.FileDir},
			true,
		},
		{
			"dir vs file mismatch",
			storage.DiscoverItem{FileType: model.FileDir},
			storage.DiscoverItem{FileType: model.FileRegular},
			false,
		},
		{
			"symlink same target",
			storage.DiscoverItem{FileType: model.FileSymlink, Attr: storage.FileAttr{SymlinkTarget: "a.txt"}},
			storage.DiscoverItem{FileType: model.FileSymlink, Attr: storage.FileAttr{SymlinkTarget: "a.txt"}},
			true,
		},
		{
			"symlink different target",
			storage.DiscoverItem{FileType: model.FileSymlink, Attr: storage.FileAttr{SymlinkTarget: "a.txt"}},
			storage.DiscoverItem{FileType: model.FileSymlink, Attr: storage.FileAttr{SymlinkTarget: "b.txt"}},
			false,
		},
		{
			"regular same mtime+size",
			storage.DiscoverItem{FileType: model.FileRegular, Size: 100, Attr: storage.FileAttr{Mtime: t1}},
			storage.DiscoverItem{FileType: model.FileRegular, Size: 100, Attr: storage.FileAttr{Mtime: t1}},
			true,
		},
		{
			"regular different size",
			storage.DiscoverItem{FileType: model.FileRegular, Size: 100, Attr: storage.FileAttr{Mtime: t1}},
			storage.DiscoverItem{FileType: model.FileRegular, Size: 200, Attr: storage.FileAttr{Mtime: t1}},
			false,
		},
		{
			"regular different mtime",
			storage.DiscoverItem{FileType: model.FileRegular, Size: 100, Attr: storage.FileAttr{Mtime: t1}},
			storage.DiscoverItem{FileType: model.FileRegular, Size: 100, Attr: storage.FileAttr{Mtime: t2}},
			false,
		},
		{
			"regular same etag",
			storage.DiscoverItem{FileType: model.FileRegular, Size: 100, Checksum: []byte("abc"), Algorithm: "etag-md5"},
			storage.DiscoverItem{FileType: model.FileRegular, Size: 100, Checksum: []byte("abc"), Algorithm: "etag-md5"},
			true,
		},
		{
			"regular different etag",
			storage.DiscoverItem{FileType: model.FileRegular, Size: 100, Checksum: []byte("abc"), Algorithm: "etag-md5"},
			storage.DiscoverItem{FileType: model.FileRegular, Size: 100, Checksum: []byte("def"), Algorithm: "etag-md5"},
			false,
		},
		{
			"regular etag beats mtime",
			storage.DiscoverItem{FileType: model.FileRegular, Size: 100, Attr: storage.FileAttr{Mtime: t1}, Checksum: []byte("abc"), Algorithm: "etag-md5"},
			storage.DiscoverItem{FileType: model.FileRegular, Size: 100, Attr: storage.FileAttr{Mtime: t1}, Checksum: []byte("abc"), Algorithm: "etag-md5"},
			true,
		},
		{
			"regular etag mismatch even if mtime matches",
			storage.DiscoverItem{FileType: model.FileRegular, Size: 100, Attr: storage.FileAttr{Mtime: t1}, Checksum: []byte("abc"), Algorithm: "etag-md5"},
			storage.DiscoverItem{FileType: model.FileRegular, Size: 100, Attr: storage.FileAttr{Mtime: t1}, Checksum: []byte("def"), Algorithm: "etag-md5"},
			false,
		},
		{
			"regular zero mtime no etag",
			storage.DiscoverItem{FileType: model.FileRegular, Size: 100},
			storage.DiscoverItem{FileType: model.FileRegular, Size: 100},
			false,
		},
		{
			"regular one side etag other side mtime",
			storage.DiscoverItem{FileType: model.FileRegular, Size: 100, Checksum: []byte("abc"), Algorithm: "etag-md5"},
			storage.DiscoverItem{FileType: model.FileRegular, Size: 100, Attr: storage.FileAttr{Mtime: t1}},
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
