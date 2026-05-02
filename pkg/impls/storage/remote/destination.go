package remote

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/pkg/interfaces/storage"
	"github.com/zp001/ncp/pkg/model"
)

// Destination implements storage.Destination for remote ncp servers.
// Each Destination owns a single TCP connection used for all operations.
type Destination struct {
	conn     *protocol.Conn
	basePath string // destination base path on the remote server
}

var _ storage.Destination = (*Destination)(nil)

// NewDestination dials the remote server and returns a Destination ready for use.
func NewDestination(addr, basePath string) (*Destination, error) {
	conn, err := protocol.Dial(addr)
	if err != nil {
		return nil, fmt.Errorf("remote destination dial %s: %w", addr, err)
	}
	return &Destination{conn: conn, basePath: basePath}, nil
}

// OpenFile sends MsgOpen and returns a Writer backed by the shared connection.
func (d *Destination) OpenFile(relPath string, size int64, mode os.FileMode, uid, gid int) (storage.Writer, error) {
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
func (d *Destination) Mkdir(relPath string, mode os.FileMode, uid, gid int) error {
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
func (d *Destination) Symlink(relPath string, target string) error {
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
func (d *Destination) SetMetadata(relPath string, meta model.FileMetadata) error {
	fullPath := d.fullPath(relPath)

	// Utime
	utimeMsg := &protocol.UtimeMsg{
		Path:  fullPath,
		Atime: meta.Atime,
		Mtime: meta.Mtime,
	}
	if _, err := d.conn.SendMsgRecvAck(protocol.MsgUtime, utimeMsg.Encode()); err != nil {
		return fmt.Errorf("remote utime %s: %w", relPath, err)
	}

	// Xattr
	for key, value := range meta.Xattr {
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

// Done sends MsgTaskDone to the server and closes the connection.
func (d *Destination) Done() error {
	_, err := d.conn.SendMsgRecvAck(protocol.MsgTaskDone, nil)
	d.conn.Close()
	return err
}

func (d *Destination) fullPath(relPath string) string {
	if d.basePath == "" {
		return relPath
	}
	return filepath.Join(d.basePath, relPath)
}
