package remote

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// SourceOption configures a Source on creation.
type SourceOption func(*Source)

// WithMode sets the protocol mode for InitMsg (ModeSource or ModeSourceNoWalker).
func WithMode(mode uint8) SourceOption {
	return func(s *Source) { s.mode = mode }
}

// Source implements storage.Source for remote ncp servers.
type Source struct {
	addr     string         // server address (host:port)
	basePath string         // URL path (e.g. "/data/backup")
	conn     *protocol.Conn // instance-level single connection
	mode     uint8          // ModeSource or ModeSourceNoWalker
}

var _ storage.Source = (*Source)(nil)

// NewSource creates a remote Source for the given ncp server.
// The connection is not established until BeginTask is called.
func NewSource(addr, basePath string, opts ...SourceOption) (*Source, error) {
	s := &Source{addr: addr, basePath: basePath, mode: protocol.ModeSource}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

// dialAndInit establishes a fresh TCP connection and sends MsgInit.
func (s *Source) dialAndInit(taskID string) (*protocol.Conn, error) {
	conn, err := protocol.Dial(s.addr)
	if err != nil {
		return nil, fmt.Errorf("remote source dial %s: %w", s.addr, err)
	}
	initMsg := &protocol.InitMsg{BasePath: s.basePath, Mode: s.mode, TaskID: taskID}
	if _, err := conn.SendMsgRecvAck(protocol.MsgInit, initMsg.Encode()); err != nil {
		conn.Close()
		return nil, fmt.Errorf("remote source init %s: %w", s.addr, err)
	}
	return conn, nil
}

// BeginTask establishes the TCP connection and sends MsgInit.
func (s *Source) BeginTask(ctx context.Context, taskID string) error {
	conn, err := s.dialAndInit(taskID)
	if err != nil {
		return err
	}
	s.conn = conn
	return nil
}

// EndTask closes the underlying connection.
func (s *Source) EndTask(ctx context.Context, summary storage.TaskSummary) error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

// Walk sends MsgList requests with pagination and calls fn for each entry.
func (s *Source) Walk(ctx context.Context, fn func(context.Context, storage.DiscoverItem) error) error {
	if s.conn == nil {
		return fmt.Errorf("remote source not connected")
	}

	token := ""
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		listMsg := &protocol.ListMsg{ContinuationToken: token}
		f, err := s.conn.SendAndRecv(protocol.MsgList, listMsg.Encode())
		if err != nil {
			return fmt.Errorf("remote list: %w", err)
		}
		if f.Type == protocol.MsgError {
			emsg := &protocol.ErrorMsg{}
			if derr := emsg.Decode(f.Payload); derr != nil {
				return fmt.Errorf("remote list error (undecodable): %w", derr)
			}
			return fmt.Errorf("remote list error: code=0x%04X msg=%s", emsg.Code, emsg.Message)
		}
		if f.Type != protocol.MsgData {
			return fmt.Errorf("remote list: unexpected response type 0x%02X", f.Type)
		}

		dataMsg := &protocol.DataMsg{}
		if err := dataMsg.Decode(f.Payload); err != nil {
			return fmt.Errorf("remote list decode: %w", err)
		}

		for i := range dataMsg.Entries {
			item := listEntryToDiscoverItem(&dataMsg.Entries[i])
			if err := fn(ctx, item); err != nil {
				return err
			}
		}

		if dataMsg.ContinuationToken == "" {
			break
		}
		token = dataMsg.ContinuationToken
	}

	return nil
}

// Open sends MsgOpen with O_RDONLY and returns a FileReader backed by the shared connection.
func (s *Source) Open(ctx context.Context, relPath string) (storage.FileReader, error) {
	if s.conn == nil {
		return nil, fmt.Errorf("remote source not connected")
	}

	msg := &protocol.OpenMsg{
		Path:  relPath,
		Flags: protocol.ProtoO_RDONLY,
	}
	ack, err := s.conn.SendMsgRecvAck(protocol.MsgOpen, msg.Encode())
	if err != nil {
		return nil, fmt.Errorf("remote open %s: %w", relPath, err)
	}
	_, fd := protocol.DecodeAckFD(ack.Data)

	return &Reader{conn: s.conn, fd: fd}, nil
}

// Stat sends MsgStat and returns the file metadata as a DiscoverItem.
func (s *Source) Stat(ctx context.Context, relPath string) (storage.DiscoverItem, error) {
	if s.conn == nil {
		return storage.DiscoverItem{}, fmt.Errorf("remote source not connected")
	}

	msg := &protocol.StatMsg{RelPath: relPath}
	f, err := s.conn.SendAndRecv(protocol.MsgStat, msg.Encode())
	if err != nil {
		return storage.DiscoverItem{}, fmt.Errorf("remote stat %s: %w", relPath, err)
	}
	if f.Type == protocol.MsgError {
		emsg := &protocol.ErrorMsg{}
		if derr := emsg.Decode(f.Payload); derr != nil {
			return storage.DiscoverItem{}, fmt.Errorf("remote stat error (undecodable): %w", derr)
		}
		return storage.DiscoverItem{}, fmt.Errorf("remote stat error: code=0x%04X msg=%s", emsg.Code, emsg.Message)
	}

	dataMsg := &protocol.DataMsg{}
	if err := dataMsg.Decode(f.Payload); err != nil {
		return storage.DiscoverItem{}, fmt.Errorf("remote stat decode: %w", err)
	}

	if len(dataMsg.Entries) == 0 {
		return storage.DiscoverItem{}, fmt.Errorf("remote stat %s: no entry returned", relPath)
	}

	return listEntryToDiscoverItem(&dataMsg.Entries[0]), nil
}

// URI returns the source base as an ncp:// URL.
func (s *Source) URI() string {
	return "ncp://" + s.addr + s.basePath
}

// protoModeToOS converts POSIX protocol mode bits to Go os.FileMode.
// The protocol uses POSIX values (bit 9-11 for setuid/setgid/sticky),
// which can be used directly with os.Chmod as os.FileMode(posixValue).
func protoModeToOS(pm uint32) os.FileMode {
	return os.FileMode(pm)
}

// listEntryToDiscoverItem converts a protocol ListEntry to a DiscoverItem.
func listEntryToDiscoverItem(e *protocol.ListEntry) storage.DiscoverItem {
	item := storage.DiscoverItem{
		RelPath:  e.RelPath,
		FileType: model.FileType(e.FileType),
		Size:     e.FileSize,
		Attr: storage.FileAttr{
			Mode:          protoModeToOS(e.Mode),
			Uid:           int(e.UID),
			Gid:           int(e.GID),
			SymlinkTarget: e.LinkTarget,
		},
	}
	if e.Mtime != 0 {
		item.Attr.Mtime = time.Unix(0, e.Mtime)
	}
	item.Checksum, item.Algorithm = parseETag(e.ETag)
	return item
}

// parseETag converts a remote ETag string into (checksum, algorithm).
func parseETag(etag string) ([]byte, string) {
	etag = strings.ToLower(strings.Trim(etag, `"`))
	if etag == "" {
		return nil, ""
	}
	if strings.Contains(etag, "-") {
		return []byte(etag), "etag-multipart"
	}
	if b, err := hex.DecodeString(etag); err == nil {
		return b, "etag-md5"
	}
	return []byte(etag), "etag-multipart"
}
