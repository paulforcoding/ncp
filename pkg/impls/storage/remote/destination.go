package remote

import (
	"context"
	"fmt"
	"os"

	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/pkg/interfaces/storage"
)

// Destination implements storage.Destination for remote ncp servers.
type Destination struct {
	addr     string
	basePath string
	conn     *protocol.Conn
}

var _ storage.Destination = (*Destination)(nil)

// NewDestination creates a remote Destination for the given ncp server.
// The connection is not established until Open is called.
func NewDestination(addr, basePath string) (*Destination, error) {
	return &Destination{addr: addr, basePath: basePath}, nil
}

// BeginTask establishes the TCP connection and sends MsgInit.
func (d *Destination) BeginTask(ctx context.Context, taskID string) error {
	conn, err := protocol.Dial(d.addr)
	if err != nil {
		return fmt.Errorf("remote destination dial %s: %w", d.addr, err)
	}

	initMsg := &protocol.InitMsg{BasePath: d.basePath, Mode: protocol.ModeDestination, TaskID: taskID}
	if _, err := conn.SendMsgRecvAck(protocol.MsgInit, initMsg.Encode()); err != nil {
		conn.Close()
		return fmt.Errorf("remote init %s: %w", d.addr, err)
	}

	d.conn = conn
	return nil
}

// EndTask closes the underlying connection.
func (d *Destination) EndTask(ctx context.Context, summary storage.TaskSummary) error {
	if d.conn != nil {
		return d.conn.Close()
	}
	return nil
}

// OpenFile sends MsgOpen and returns a Writer backed by the shared connection.
func (d *Destination) OpenFile(ctx context.Context, relPath string, size int64, mode os.FileMode, uid, gid int) (storage.FileWriter, error) {
	if d.conn == nil {
		return nil, fmt.Errorf("remote destination not connected")
	}
	fullPath := d.fullPath(relPath)
	msg := &protocol.OpenMsg{
		Path:  fullPath,
		Flags: uint32(os.O_WRONLY | os.O_CREATE | os.O_TRUNC),
		Mode:  uint32(mode),
		UID:   uint32(uid),
		GID:   uint32(gid),
	}
	ack, err := d.conn.SendMsgRecvAck(protocol.MsgOpen, msg.Encode())
	if err != nil {
		return nil, fmt.Errorf("remote openfile %s: %w", relPath, err)
	}
	_, fd := protocol.DecodeAckFD(ack.Data)
	return &Writer{conn: d.conn, fd: fd}, nil
}

// Mkdir sends MsgMkdir to the server.
func (d *Destination) Mkdir(ctx context.Context, relPath string, mode os.FileMode, uid, gid int) error {
	if d.conn == nil {
		return fmt.Errorf("remote destination not connected")
	}
	msg := &protocol.MkdirMsg{
		Path: d.fullPath(relPath),
		Mode: uint32(mode),
		UID:  uint32(uid),
		GID:  uint32(gid),
	}
	_, err := d.conn.SendMsgRecvAck(protocol.MsgMkdir, msg.Encode())
	if err != nil {
		return fmt.Errorf("remote mkdir %s: %w", relPath, err)
	}
	return nil
}

// Symlink sends MsgSymlink to the server.
func (d *Destination) Symlink(ctx context.Context, relPath string, target string) error {
	if d.conn == nil {
		return fmt.Errorf("remote destination not connected")
	}
	msg := &protocol.SymlinkMsg{
		Target:   target,
		LinkPath: d.fullPath(relPath),
	}
	_, err := d.conn.SendMsgRecvAck(protocol.MsgSymlink, msg.Encode())
	if err != nil {
		return fmt.Errorf("remote symlink %s: %w", relPath, err)
	}
	return nil
}

// SetMetadata sends MsgUtime and MsgSetxattr to the server.
func (d *Destination) SetMetadata(ctx context.Context, relPath string, attr storage.FileAttr) error {
	if d.conn == nil {
		return fmt.Errorf("remote destination not connected")
	}
	fullPath := d.fullPath(relPath)

	// Utime
	var atime, mtime int64
	if !attr.Atime.IsZero() {
		atime = attr.Atime.UnixNano()
	}
	if !attr.Mtime.IsZero() {
		mtime = attr.Mtime.UnixNano()
	}
	utimeMsg := &protocol.UtimeMsg{
		Path:  fullPath,
		Atime: atime,
		Mtime: mtime,
	}
	if _, err := d.conn.SendMsgRecvAck(protocol.MsgUtime, utimeMsg.Encode()); err != nil {
		return fmt.Errorf("remote utime %s: %w", relPath, err)
	}

	// Xattr
	for key, value := range attr.Xattr {
		xattrMsg := &protocol.SetxattrMsg{
			Path:  fullPath,
			Key:   key,
			Value: value,
		}
		if _, err := d.conn.SendMsgRecvAck(protocol.MsgSetxattr, xattrMsg.Encode()); err != nil {
			return fmt.Errorf("remote setxattr %s %s: %w", relPath, key, err)
		}
	}

	return nil
}

// Stat sends MsgStat and returns the file metadata as a DiscoverItem.
// Used by skip-by-mtime to check if dst already has the file.
func (d *Destination) Stat(ctx context.Context, relPath string) (storage.DiscoverItem, error) {
	if d.conn == nil {
		return storage.DiscoverItem{}, fmt.Errorf("remote destination not connected")
	}
	msg := &protocol.StatMsg{RelPath: d.fullPath(relPath)}
	f, err := d.conn.SendAndRecv(protocol.MsgStat, msg.Encode())
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
		return storage.DiscoverItem{}, fmt.Errorf("remote stat %s: %w", relPath, storage.ErrNotFound)
	}
	return listEntryToDiscoverItem(&dataMsg.Entries[0]), nil
}

// fullPath returns relPath — basePath is already sent via MsgInit.
func (d *Destination) fullPath(relPath string) string {
	return relPath
}
