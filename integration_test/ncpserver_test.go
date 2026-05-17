//go:build integration

package integration

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zp001/ncp/internal/ncpserver"
	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/pkg/impls/storage/remote"
	"github.com/zp001/ncp/pkg/interfaces/storage"
)

func TestNcpserverList(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "subdir"), 0o755)
	os.WriteFile(filepath.Join(root, "file1.txt"), []byte("hello"), 0o644)
	os.WriteFile(filepath.Join(root, "subdir", "file2.txt"), []byte("world"), 0o644)
	os.Symlink("file1.txt", filepath.Join(root, "link1"))

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	srv := ncpserver.NewServer(ln)
	go srv.Serve()
	defer srv.Shutdown()

	conn, _ := protocol.Dial(ln.Addr().String())
	defer conn.Close()

	initMsg := &protocol.InitMsg{BasePath: root, Mode: protocol.ModeSource, TaskID: "test", ConfigJSON: `{"ProgramLogLevel":"info"}`}
	_, err := conn.SendMsgRecvAck(protocol.MsgInit, initMsg.Encode())
	if err != nil {
		t.Fatalf("init: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	listMsg := &protocol.ListMsg{ContinuationToken: ""}
	f, err := conn.SendAndRecv(protocol.MsgList, listMsg.Encode())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	dm := &protocol.DataMsg{}
	if err := dm.Decode(f.Payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, e := range dm.Entries {
		t.Logf("entry: %s type=%d", e.RelPath, e.FileType)
	}
	t.Logf("token=%q", dm.ContinuationToken)
}

func TestNcpserverRemoteWalk(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "subdir"), 0o755)
	os.WriteFile(filepath.Join(root, "file1.txt"), []byte("hello"), 0o644)
	os.WriteFile(filepath.Join(root, "subdir", "file2.txt"), []byte("world"), 0o644)
	os.Symlink("file1.txt", filepath.Join(root, "link1"))

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	srv := ncpserver.NewServer(ln)
	go srv.Serve()
	defer srv.Shutdown()

	src, _ := remote.NewSource(ln.Addr().String(), root)
	ctx := context.Background()
	if err := src.BeginTask(ctx, "test-task"); err != nil {
		t.Fatalf("begin task: %v", err)
	}
	defer src.EndTask(ctx, storage.TaskSummary{})

	var paths []string
	err := src.Walk(ctx, func(_ context.Context, item storage.DiscoverItem) error {
		paths = append(paths, item.RelPath)
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	for _, p := range paths {
		t.Logf("walked: %s", p)
	}
	if len(paths) != 4 {
		t.Fatalf("expected 4 entries, got %d: %v", len(paths), paths)
	}
}
