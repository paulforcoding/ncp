//go:build integration

package copy

import (
	"context"
	"crypto/md5"
	"hash"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/internal/storage/local"
	"github.com/zp001/ncp/internal/storage/remote"
	"github.com/zp001/ncp/pkg/model"
	"github.com/zp001/ncp/pkg/storage"
	"github.com/zp001/ncp/testutil"
)

// serveConnHandler is a simplified version of the production serve handler
// for integration testing.
type testServeHandler struct {
	dst *local.Destination
}

func (h *testServeHandler) HandleConn(conn *protocol.Conn) error {
	fdMap := make(map[uint32]*testOpenFile)
	var nextFD uint32
	defer func() {
		for _, of := range fdMap {
			of.writer.Close(nil)
		}
	}()

	for {
		frame, err := conn.Recv()
		if err != nil {
			return err
		}

		var respType uint8
		var respPayload []byte

		switch frame.Type {
		case protocol.MsgOpen:
			msg := &protocol.OpenMsg{}
			msg.Decode(frame.Payload)
			writer, err := h.dst.OpenFile(msg.Path, 0, os.FileMode(msg.Mode), int(msg.UID), int(msg.GID))
			if err != nil {
				respType = protocol.MsgError
				respPayload = protocol.EncodeError(model.ErrFileOpen, err.Error())
			} else {
				fd := nextFD
				nextFD++
				fdMap[fd] = newTestOpenFile(writer)
				respType = protocol.MsgAck
				respPayload = protocol.EncodeAckFD(0, fd)
			}

		case protocol.MsgPwrite:
			msg := &protocol.PwriteMsg{}
			msg.Decode(frame.Payload)
			of, ok := fdMap[msg.FD]
			if !ok {
				respType = protocol.MsgError
				respPayload = protocol.EncodeError(model.ErrFileWrite, "bad fd")
			} else {
				n, err := of.writer.WriteAt(msg.Data, msg.Offset)
				if err != nil {
					respType = protocol.MsgError
					respPayload = protocol.EncodeError(model.ErrFileWrite, err.Error())
				} else {
					of.md5Write(msg.Data)
					respType = protocol.MsgAck
					respPayload = protocol.EncodeAckU32(0, uint32(n))
				}
			}

		case protocol.MsgClose:
			msg := &protocol.CloseMsg{}
			msg.Decode(frame.Payload)
			of, ok := fdMap[msg.FD]
			if !ok {
				respType = protocol.MsgError
				respPayload = protocol.EncodeError(model.ErrFileOpen, "bad fd")
			} else {
				serverMD5 := of.md5Sum()
				of.writer.Close(nil)
				delete(fdMap, msg.FD)
				if !equalBytes(msg.Checksum, serverMD5) {
					respType = protocol.MsgError
					respPayload = protocol.EncodeError(model.ErrChecksumMismatch, "checksum mismatch")
				} else {
					respType = protocol.MsgAck
					respPayload = (&protocol.AckMsg{ResultCode: 0}).Encode()
				}
			}

		case protocol.MsgMkdir:
			msg := &protocol.MkdirMsg{}
			msg.Decode(frame.Payload)
			err := h.dst.Mkdir(msg.Path, os.FileMode(msg.Mode), int(msg.UID), int(msg.GID))
			if err != nil {
				respType = protocol.MsgError
				respPayload = protocol.EncodeError(model.ErrFileMkdir, err.Error())
			} else {
				respType = protocol.MsgAck
				respPayload = (&protocol.AckMsg{ResultCode: 0}).Encode()
			}

		case protocol.MsgSymlink:
			msg := &protocol.SymlinkMsg{}
			msg.Decode(frame.Payload)
			err := h.dst.Symlink(msg.LinkPath, msg.Target)
			if err != nil {
				respType = protocol.MsgError
				respPayload = protocol.EncodeError(model.ErrFileSymlink, err.Error())
			} else {
				respType = protocol.MsgAck
				respPayload = (&protocol.AckMsg{ResultCode: 0}).Encode()
			}

		case protocol.MsgUtime:
			respType = protocol.MsgAck
			respPayload = (&protocol.AckMsg{ResultCode: 0}).Encode()

		case protocol.MsgSetxattr:
			respType = protocol.MsgAck
			respPayload = (&protocol.AckMsg{ResultCode: 0}).Encode()

		case protocol.MsgTaskDone:
			conn.Send(protocol.MsgAck, (&protocol.AckMsg{ResultCode: 0}).Encode())
			return nil

		default:
			respType = protocol.MsgError
			respPayload = protocol.EncodeError(model.ErrProtocol, "unknown message type")
		}

		if err := conn.Send(respType, respPayload); err != nil {
			return err
		}
	}
}

type testOpenFile struct {
	writer storage.Writer
	md5    hash.Hash
}

func newTestOpenFile(writer storage.Writer) *testOpenFile {
	return &testOpenFile{writer: writer, md5: md5.New()}
}

func (f *testOpenFile) md5Write(data []byte) {
	f.md5.Write(data)
}

func (f *testOpenFile) md5Sum() []byte {
	return f.md5.Sum(nil)
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Test 12: Remote copy — basic files, directories, symlinks via protocol
func TestIntegration_RemoteBasicCopy(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	// Create source tree
	os.MkdirAll(filepath.Join(src, "subdir"), 0o755)
	os.WriteFile(filepath.Join(src, "file1.txt"), []byte("hello"), 0o644)
	os.WriteFile(filepath.Join(src, "subdir", "file2.txt"), []byte("world"), 0o644)
	os.Symlink("file1.txt", filepath.Join(src, "link1"))

	// Start server
	localDst, err := local.NewDestination(dst)
	if err != nil {
		t.Fatalf("create local dst: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := protocol.NewServer(ln, func() protocol.ConnHandler {
		return &testServeHandler{dst: localDst}
	})
	go srv.Serve()
	defer srv.Close()

	// Create source and remote destination
	srcObj, err := local.NewSource(src)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}

	store := openTestStore(t)

	// Create remote destination factory
	addr := ln.Addr().String()
	dstFactory := func(id int) (storage.Destination, error) {
		return remote.NewDestination(addr, "") // empty basePath — server handles path resolution
	}

	job := NewJob(srcObj, nil, store,
		WithParallelism(1),
		WithDstFactory(dstFactory),
		WithEnsureDirMtime(false),
		WithDstBase("ncp://"+addr),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	// Verify file content
	data, err := os.ReadFile(filepath.Join(dst, "file1.txt"))
	if err != nil || string(data) != "hello" {
		t.Fatalf("file1.txt content mismatch: %q, err %v", string(data), err)
	}
	data, err = os.ReadFile(filepath.Join(dst, "subdir", "file2.txt"))
	if err != nil || string(data) != "world" {
		t.Fatalf("file2.txt content mismatch: %q, err %v", string(data), err)
	}

	// Verify symlink
	target, err := os.Readlink(filepath.Join(dst, "link1"))
	if err != nil || target != "file1.txt" {
		t.Fatalf("symlink target mismatch: got %q, err %v", target, err)
	}

	// Verify directory
	if _, err := os.Stat(filepath.Join(dst, "subdir")); err != nil {
		t.Fatalf("subdir missing: %v", err)
	}
}

// Test 13: Remote copy with parallelism
func TestIntegration_RemoteParallelCopy(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	if err := testutil.CreateTestTree(src, 200); err != nil {
		t.Fatalf("create test tree: %v", err)
	}

	// Start server
	localDst, err := local.NewDestination(dst)
	if err != nil {
		t.Fatalf("create local dst: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := protocol.NewServer(ln, func() protocol.ConnHandler {
		return &testServeHandler{dst: localDst}
	})
	go srv.Serve()
	defer srv.Close()

	srcObj, _ := local.NewSource(src)
	store := openTestStore(t)
	addr := ln.Addr().String()

	dstFactory := func(id int) (storage.Destination, error) {
		return remote.NewDestination(addr, "")
	}

	job := NewJob(srcObj, nil, store,
		WithParallelism(4),
		WithDstFactory(dstFactory),
		WithEnsureDirMtime(false),
		WithDstBase("ncp://"+addr),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	exitCode, err := job.Run(ctx)
	if err != nil {
		t.Fatalf("copy job: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", exitCode)
	}

	if err := testutil.VerifyCopy(src, dst); err != nil {
		t.Fatalf("verify copy: %v", err)
	}
}
