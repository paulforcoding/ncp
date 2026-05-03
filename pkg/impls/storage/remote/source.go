package remote

import (
	"context"
	"fmt"

	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// Source implements storage.Source for remote ncp servers.
type Source struct {
	addr     string // server address (host:port)
	basePath string // URL path (e.g. "/data/backup")
}

var _ storage.Source = (*Source)(nil)

// NewSource creates a remote Source for the given ncp server.
func NewSource(addr, basePath string) (*Source, error) {
	return &Source{addr: addr, basePath: basePath}, nil
}

// dialAndInit establishes a TCP connection and sends MsgInit.
func (s *Source) dialAndInit() (*protocol.Conn, error) {
	conn, err := protocol.Dial(s.addr)
	if err != nil {
		return nil, fmt.Errorf("remote source dial %s: %w", s.addr, err)
	}
	initMsg := &protocol.InitMsg{BasePath: s.basePath}
	if _, err := conn.SendMsgRecvAck(protocol.MsgInit, initMsg.Encode()); err != nil {
		conn.Close()
		return nil, fmt.Errorf("remote source init %s: %w", s.addr, err)
	}
	return conn, nil
}

// Walk sends MsgList requests with pagination and calls fn for each entry.
func (s *Source) Walk(ctx context.Context, fn func(model.DiscoverItem) error) error {
	conn, err := s.dialAndInit()
	if err != nil {
		return err
	}
	defer conn.Close()

	token := ""
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		listMsg := &protocol.ListMsg{ContinuationToken: token}
		f, err := conn.SendAndRecv(protocol.MsgList, listMsg.Encode())
		if err != nil {
			return fmt.Errorf("remote list: %w", err)
		}
		if f.Type == protocol.MsgError {
			emsg := &protocol.ErrorMsg{}
			emsg.Decode(f.Payload)
			return fmt.Errorf("remote list error: code=0x%04X msg=%s", emsg.Code, emsg.Message)
		}

		dataMsg := &protocol.DataMsg{}
		dataMsg.Decode(f.Payload)

		for i := range dataMsg.Entries {
			item := listEntryToDiscoverItem(&dataMsg.Entries[i], s)
			if err := fn(item); err != nil {
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

// Open sends MsgOpen with O_RDONLY and returns a Reader backed by a dedicated connection.
func (s *Source) Open(relPath string) (storage.Reader, error) {
	conn, err := s.dialAndInit()
	if err != nil {
		return nil, err
	}

	msg := &protocol.OpenMsg{
		Path:  relPath,
		Flags: 0, // O_RDONLY
	}
	ack, err := conn.SendMsgRecvAck(protocol.MsgOpen, msg.Encode())
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("remote open %s: %w", relPath, err)
	}
	_, fd := protocol.DecodeAckFD(ack.Data)

	return &Reader{conn: conn, fd: fd}, nil
}

// Restat sends MsgStat and returns the file metadata as a DiscoverItem.
func (s *Source) Restat(relPath string) (model.DiscoverItem, error) {
	conn, err := s.dialAndInit()
	if err != nil {
		return model.DiscoverItem{}, err
	}
	defer conn.Close()

	msg := &protocol.StatMsg{RelPath: relPath}
	f, err := conn.SendAndRecv(protocol.MsgStat, msg.Encode())
	if err != nil {
		return model.DiscoverItem{}, fmt.Errorf("remote stat %s: %w", relPath, err)
	}
	if f.Type == protocol.MsgError {
		emsg := &protocol.ErrorMsg{}
		emsg.Decode(f.Payload)
		return model.DiscoverItem{}, fmt.Errorf("remote stat error: code=0x%04X msg=%s", emsg.Code, emsg.Message)
	}

	dataMsg := &protocol.DataMsg{}
	dataMsg.Decode(f.Payload)

	if len(dataMsg.Entries) == 0 {
		return model.DiscoverItem{}, fmt.Errorf("remote stat %s: no entry returned", relPath)
	}

	return listEntryToDiscoverItem(&dataMsg.Entries[0], s), nil
}

// Base returns the source base as an ncp:// URL.
func (s *Source) Base() string {
	return "ncp://" + s.addr + s.basePath
}

// listEntryToDiscoverItem converts a protocol ListEntry to a DiscoverItem.
// uid/gid are set to 0 since they are not transmitted in the remote protocol.
func listEntryToDiscoverItem(e *protocol.ListEntry, src *Source) model.DiscoverItem {
	return model.DiscoverItem{
		SrcBase:    src.Base(),
		RelPath:    e.RelPath,
		FileType:   model.FileType(e.FileType),
		FileSize:   e.FileSize,
		Mode:       e.Mode,
		Uid:        0,
		Gid:        0,
		Mtime:      e.Mtime,
		LinkTarget: e.LinkTarget,
		ETag:       e.ETag,
	}
}
