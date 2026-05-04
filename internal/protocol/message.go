package protocol

import (
	"encoding/binary"
	"fmt"
)

// Client → Server message types (0x0X)
const (
	MsgOpen     uint8 = 0x01
	MsgPwrite   uint8 = 0x02
	MsgFsync    uint8 = 0x03
	MsgClose    uint8 = 0x04
	MsgMkdir    uint8 = 0x05
	MsgSymlink  uint8 = 0x06
	MsgUtime    uint8 = 0x07
	MsgSetxattr uint8 = 0x08
	MsgTaskDone uint8 = 0x09
	MsgInit     uint8 = 0x0A
	MsgList     uint8 = 0x0B
	MsgPread    uint8 = 0x0C
	MsgStat     uint8 = 0x0D
)

// Server → Client message types (0x8X)
const (
	MsgAck   uint8 = 0x81
	MsgError uint8 = 0x82
	MsgData  uint8 = 0x83
)

// --- Client → Server messages ---

// OpenMsg is the payload for MsgOpen.
type OpenMsg struct {
	Path  string
	Flags uint32
	Mode  uint32
	UID   uint32
	GID   uint32
}

func (m *OpenMsg) Encode() []byte {
	pathBytes := []byte(m.Path)
	n := 2 + len(pathBytes) + 4 + 4 + 4 + 4
	b := make([]byte, n)
	off := 0
	binary.BigEndian.PutUint16(b[off:], uint16(len(pathBytes)))
	off += 2
	copy(b[off:], pathBytes)
	off += len(pathBytes)
	binary.BigEndian.PutUint32(b[off:], m.Flags)
	off += 4
	binary.BigEndian.PutUint32(b[off:], m.Mode)
	off += 4
	binary.BigEndian.PutUint32(b[off:], m.UID)
	off += 4
	binary.BigEndian.PutUint32(b[off:], m.GID)
	return b
}

func (m *OpenMsg) Decode(data []byte) error {
	off := 0
	pathLen := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	m.Path = string(data[off : off+pathLen])
	off += pathLen
	m.Flags = binary.BigEndian.Uint32(data[off:])
	off += 4
	m.Mode = binary.BigEndian.Uint32(data[off:])
	off += 4
	m.UID = binary.BigEndian.Uint32(data[off:])
	off += 4
	m.GID = binary.BigEndian.Uint32(data[off:])
	return nil
}

// PwriteMsg is the payload for MsgPwrite.
type PwriteMsg struct {
	FD     uint32
	Offset int64
	Data   []byte
}

func (m *PwriteMsg) Encode() []byte {
	n := 4 + 8 + 4 + len(m.Data)
	b := make([]byte, n)
	off := 0
	binary.BigEndian.PutUint32(b[off:], m.FD)
	off += 4
	binary.BigEndian.PutUint64(b[off:], uint64(m.Offset))
	off += 8
	binary.BigEndian.PutUint32(b[off:], uint32(len(m.Data)))
	off += 4
	copy(b[off:], m.Data)
	return b
}

func (m *PwriteMsg) Decode(data []byte) error {
	off := 0
	m.FD = binary.BigEndian.Uint32(data[off:])
	off += 4
	m.Offset = int64(binary.BigEndian.Uint64(data[off:]))
	off += 8
	dataLen := int(binary.BigEndian.Uint32(data[off:]))
	off += 4
	m.Data = make([]byte, dataLen)
	copy(m.Data, data[off:off+dataLen])
	return nil
}

// FsyncMsg is the payload for MsgFsync.
type FsyncMsg struct {
	FD uint32
}

func (m *FsyncMsg) Encode() []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, m.FD)
	return b
}

func (m *FsyncMsg) Decode(data []byte) error {
	m.FD = binary.BigEndian.Uint32(data)
	return nil
}

// CloseMsg is the payload for MsgClose.
type CloseMsg struct {
	FD       uint32
	Checksum []byte
}

func (m *CloseMsg) Encode() []byte {
	n := 4 + 4 + len(m.Checksum)
	b := make([]byte, n)
	off := 0
	binary.BigEndian.PutUint32(b[off:], m.FD)
	off += 4
	binary.BigEndian.PutUint32(b[off:], uint32(len(m.Checksum)))
	off += 4
	copy(b[off:], m.Checksum)
	return b
}

func (m *CloseMsg) Decode(data []byte) error {
	off := 0
	m.FD = binary.BigEndian.Uint32(data[off:])
	off += 4
	csLen := int(binary.BigEndian.Uint32(data[off:]))
	off += 4
	m.Checksum = make([]byte, csLen)
	copy(m.Checksum, data[off:off+csLen])
	return nil
}

// MkdirMsg is the payload for MsgMkdir.
type MkdirMsg struct {
	Path string
	Mode uint32
	UID  uint32
	GID  uint32
}

func (m *MkdirMsg) Encode() []byte {
	pathBytes := []byte(m.Path)
	n := 2 + len(pathBytes) + 4 + 4 + 4
	b := make([]byte, n)
	off := 0
	binary.BigEndian.PutUint16(b[off:], uint16(len(pathBytes)))
	off += 2
	copy(b[off:], pathBytes)
	off += len(pathBytes)
	binary.BigEndian.PutUint32(b[off:], m.Mode)
	off += 4
	binary.BigEndian.PutUint32(b[off:], m.UID)
	off += 4
	binary.BigEndian.PutUint32(b[off:], m.GID)
	return b
}

func (m *MkdirMsg) Decode(data []byte) error {
	off := 0
	pathLen := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	m.Path = string(data[off : off+pathLen])
	off += pathLen
	m.Mode = binary.BigEndian.Uint32(data[off:])
	off += 4
	m.UID = binary.BigEndian.Uint32(data[off:])
	off += 4
	m.GID = binary.BigEndian.Uint32(data[off:])
	return nil
}

// SymlinkMsg is the payload for MsgSymlink.
type SymlinkMsg struct {
	Target   string
	LinkPath string
}

func (m *SymlinkMsg) Encode() []byte {
	targetBytes := []byte(m.Target)
	linkBytes := []byte(m.LinkPath)
	n := 2 + len(targetBytes) + 2 + len(linkBytes)
	b := make([]byte, n)
	off := 0
	binary.BigEndian.PutUint16(b[off:], uint16(len(targetBytes)))
	off += 2
	copy(b[off:], targetBytes)
	off += len(targetBytes)
	binary.BigEndian.PutUint16(b[off:], uint16(len(linkBytes)))
	off += 2
	copy(b[off:], linkBytes)
	return b
}

func (m *SymlinkMsg) Decode(data []byte) error {
	off := 0
	targetLen := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	m.Target = string(data[off : off+targetLen])
	off += targetLen
	linkLen := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	m.LinkPath = string(data[off : off+linkLen])
	return nil
}

// UtimeMsg is the payload for MsgUtime.
type UtimeMsg struct {
	Path  string
	Atime int64
	Mtime int64
}

func (m *UtimeMsg) Encode() []byte {
	pathBytes := []byte(m.Path)
	n := 2 + len(pathBytes) + 8 + 8
	b := make([]byte, n)
	off := 0
	binary.BigEndian.PutUint16(b[off:], uint16(len(pathBytes)))
	off += 2
	copy(b[off:], pathBytes)
	off += len(pathBytes)
	binary.BigEndian.PutUint64(b[off:], uint64(m.Atime))
	off += 8
	binary.BigEndian.PutUint64(b[off:], uint64(m.Mtime))
	return b
}

func (m *UtimeMsg) Decode(data []byte) error {
	off := 0
	pathLen := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	m.Path = string(data[off : off+pathLen])
	off += pathLen
	m.Atime = int64(binary.BigEndian.Uint64(data[off:]))
	off += 8
	m.Mtime = int64(binary.BigEndian.Uint64(data[off:]))
	return nil
}

// SetxattrMsg is the payload for MsgSetxattr.
type SetxattrMsg struct {
	Path  string
	Key   string
	Value string
}

func (m *SetxattrMsg) Encode() []byte {
	pathBytes := []byte(m.Path)
	keyBytes := []byte(m.Key)
	valueBytes := []byte(m.Value)
	n := 2 + len(pathBytes) + 2 + len(keyBytes) + 4 + len(valueBytes)
	b := make([]byte, n)
	off := 0
	binary.BigEndian.PutUint16(b[off:], uint16(len(pathBytes)))
	off += 2
	copy(b[off:], pathBytes)
	off += len(pathBytes)
	binary.BigEndian.PutUint16(b[off:], uint16(len(keyBytes)))
	off += 2
	copy(b[off:], keyBytes)
	off += len(keyBytes)
	binary.BigEndian.PutUint32(b[off:], uint32(len(valueBytes)))
	off += 4
	copy(b[off:], valueBytes)
	return b
}

func (m *SetxattrMsg) Decode(data []byte) error {
	off := 0
	pathLen := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	m.Path = string(data[off : off+pathLen])
	off += pathLen
	keyLen := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	m.Key = string(data[off : off+keyLen])
	off += keyLen
	valueLen := int(binary.BigEndian.Uint32(data[off:]))
	off += 4
	m.Value = string(data[off : off+valueLen])
	return nil
}

// TaskDoneMsg is the payload for MsgTaskDone (empty).
type TaskDoneMsg struct{}

func (m *TaskDoneMsg) Encode() []byte        { return nil }
func (m *TaskDoneMsg) Decode(_ []byte) error { return nil }

// --- Server → Client messages ---

// AckMsg is the payload for MsgAck.
type AckMsg struct {
	ResultCode uint16
	Data       []byte
}

func (m *AckMsg) Encode() []byte {
	n := 2 + 4 + len(m.Data)
	b := make([]byte, n)
	off := 0
	binary.BigEndian.PutUint16(b[off:], m.ResultCode)
	off += 2
	binary.BigEndian.PutUint32(b[off:], uint32(len(m.Data)))
	off += 4
	copy(b[off:], m.Data)
	return b
}

func (m *AckMsg) Decode(data []byte) error {
	off := 0
	m.ResultCode = binary.BigEndian.Uint16(data[off:])
	off += 2
	dataLen := int(binary.BigEndian.Uint32(data[off:]))
	off += 4
	m.Data = make([]byte, dataLen)
	copy(m.Data, data[off:off+dataLen])
	return nil
}

// ErrorMsg is the payload for MsgError.
type ErrorMsg struct {
	Code    uint16
	Message string
}

func (m *ErrorMsg) Encode() []byte {
	msgBytes := []byte(m.Message)
	n := 2 + 2 + len(msgBytes)
	b := make([]byte, n)
	off := 0
	binary.BigEndian.PutUint16(b[off:], m.Code)
	off += 2
	binary.BigEndian.PutUint16(b[off:], uint16(len(msgBytes)))
	off += 2
	copy(b[off:], msgBytes)
	return b
}

func (m *ErrorMsg) Decode(data []byte) error {
	off := 0
	m.Code = binary.BigEndian.Uint16(data[off:])
	off += 2
	msgLen := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	m.Message = string(data[off : off+msgLen])
	return nil
}

// InitMsg is the payload for MsgInit — sent by client after connection to set basePath.
type InitMsg struct {
	BasePath string
}

func (m *InitMsg) Encode() []byte {
	pathBytes := []byte(m.BasePath)
	n := 2 + len(pathBytes)
	b := make([]byte, n)
	binary.BigEndian.PutUint16(b[0:], uint16(len(pathBytes)))
	copy(b[2:], pathBytes)
	return b
}

func (m *InitMsg) Decode(data []byte) error {
	pathLen := int(binary.BigEndian.Uint16(data[0:]))
	m.BasePath = string(data[2 : 2+pathLen])
	return nil
}

// ListMsg is the payload for MsgList — request directory listing.
type ListMsg struct {
	ContinuationToken string
}

func (m *ListMsg) Encode() []byte {
	tokenBytes := []byte(m.ContinuationToken)
	n := 2 + len(tokenBytes)
	b := make([]byte, n)
	binary.BigEndian.PutUint16(b[0:], uint16(len(tokenBytes)))
	copy(b[2:], tokenBytes)
	return b
}

func (m *ListMsg) Decode(data []byte) error {
	tokenLen := int(binary.BigEndian.Uint16(data[0:]))
	m.ContinuationToken = string(data[2 : 2+tokenLen])
	return nil
}

// PreadMsg is the payload for MsgPread — read data from an open file.
type PreadMsg struct {
	FD     uint32
	Offset int64
	Length uint32
}

func (m *PreadMsg) Encode() []byte {
	b := make([]byte, 4+8+4)
	binary.BigEndian.PutUint32(b[0:], m.FD)
	binary.BigEndian.PutUint64(b[4:], uint64(m.Offset))
	binary.BigEndian.PutUint32(b[12:], m.Length)
	return b
}

func (m *PreadMsg) Decode(data []byte) error {
	m.FD = binary.BigEndian.Uint32(data[0:])
	m.Offset = int64(binary.BigEndian.Uint64(data[4:]))
	m.Length = binary.BigEndian.Uint32(data[12:])
	return nil
}

// StatMsg is the payload for MsgStat — query file metadata.
type StatMsg struct {
	RelPath string
}

func (m *StatMsg) Encode() []byte {
	pathBytes := []byte(m.RelPath)
	n := 2 + len(pathBytes)
	b := make([]byte, n)
	binary.BigEndian.PutUint16(b[0:], uint16(len(pathBytes)))
	copy(b[2:], pathBytes)
	return b
}

func (m *StatMsg) Decode(data []byte) error {
	pathLen := int(binary.BigEndian.Uint16(data[0:]))
	m.RelPath = string(data[2 : 2+pathLen])
	return nil
}

// ListEntry is one record in a directory listing. No uid/gid for remote scenarios.
type ListEntry struct {
	RelPath    string
	FileType   uint8
	FileSize   int64
	Mode       uint32
	Mtime      int64
	LinkTarget string
	ETag       string
}

func (e *ListEntry) Encode() []byte {
	relBytes := []byte(e.RelPath)
	linkBytes := []byte(e.LinkTarget)
	etagBytes := []byte(e.ETag)
	n := 2 + len(relBytes) + 1 + 8 + 4 + 8 + 2 + len(linkBytes) + 2 + len(etagBytes)
	b := make([]byte, n)
	off := 0
	binary.BigEndian.PutUint16(b[off:], uint16(len(relBytes)))
	off += 2
	copy(b[off:], relBytes)
	off += len(relBytes)
	b[off] = e.FileType
	off += 1
	binary.BigEndian.PutUint64(b[off:], uint64(e.FileSize))
	off += 8
	binary.BigEndian.PutUint32(b[off:], e.Mode)
	off += 4
	binary.BigEndian.PutUint64(b[off:], uint64(e.Mtime))
	off += 8
	binary.BigEndian.PutUint16(b[off:], uint16(len(linkBytes)))
	off += 2
	copy(b[off:], linkBytes)
	off += len(linkBytes)
	binary.BigEndian.PutUint16(b[off:], uint16(len(etagBytes)))
	off += 2
	copy(b[off:], etagBytes)
	return b
}

func (e *ListEntry) Decode(data []byte) error {
	off := 0
	relLen := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	e.RelPath = string(data[off : off+relLen])
	off += relLen
	e.FileType = data[off]
	off += 1
	e.FileSize = int64(binary.BigEndian.Uint64(data[off:]))
	off += 8
	e.Mode = binary.BigEndian.Uint32(data[off:])
	off += 4
	e.Mtime = int64(binary.BigEndian.Uint64(data[off:]))
	off += 8
	linkLen := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	e.LinkTarget = string(data[off : off+linkLen])
	off += linkLen
	etagLen := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	e.ETag = string(data[off : off+etagLen])
	return nil
}

// DataMsg is the payload for MsgData — server→client data response.
// Used for both List results and Pread data.
type DataMsg struct {
	ResultCode        uint16
	Entries           []ListEntry
	ContinuationToken string
	Data              []byte
}

func (m *DataMsg) Encode() []byte {
	// Calculate total size
	n := 2 + 4 // resultCode + entryCount
	for i := range m.Entries {
		n += 4 + len(m.Entries[i].Encode()) // 4-byte length prefix per entry
	}
	tokenBytes := []byte(m.ContinuationToken)
	n += 2 + len(tokenBytes) + 4 + len(m.Data)

	b := make([]byte, n)
	off := 0
	binary.BigEndian.PutUint16(b[off:], m.ResultCode)
	off += 2
	binary.BigEndian.PutUint32(b[off:], uint32(len(m.Entries)))
	off += 4
	for i := range m.Entries {
		entryBytes := m.Entries[i].Encode()
		binary.BigEndian.PutUint32(b[off:], uint32(len(entryBytes)))
		off += 4
		copy(b[off:], entryBytes)
		off += len(entryBytes)
	}
	binary.BigEndian.PutUint16(b[off:], uint16(len(tokenBytes)))
	off += 2
	copy(b[off:], tokenBytes)
	off += len(tokenBytes)
	binary.BigEndian.PutUint32(b[off:], uint32(len(m.Data)))
	off += 4
	copy(b[off:], m.Data)
	return b
}

func (m *DataMsg) Decode(data []byte) error {
	off := 0
	m.ResultCode = binary.BigEndian.Uint16(data[off:])
	off += 2
	entryCount := int(binary.BigEndian.Uint32(data[off:]))
	off += 4
	m.Entries = make([]ListEntry, entryCount)
	for i := 0; i < entryCount; i++ {
		entryLen := int(binary.BigEndian.Uint32(data[off:]))
		off += 4
		if err := m.Entries[i].Decode(data[off : off+entryLen]); err != nil {
			return fmt.Errorf("decode entry %d: %w", i, err)
		}
		off += entryLen
	}
	tokenLen := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	m.ContinuationToken = string(data[off : off+tokenLen])
	off += tokenLen
	dataLen := int(binary.BigEndian.Uint32(data[off:]))
	off += 4
	m.Data = make([]byte, dataLen)
	copy(m.Data, data[off:off+dataLen])
	return nil
}

// EncodeAckFD creates an AckMsg carrying a file descriptor.
func EncodeAckFD(resultCode uint16, fd uint32) []byte {
	var fdBytes [4]byte
	binary.BigEndian.PutUint32(fdBytes[:], fd)
	m := &AckMsg{ResultCode: resultCode, Data: fdBytes[:]}
	return m.Encode()
}

// EncodeAckU32 creates an AckMsg carrying a uint32 value.
func EncodeAckU32(resultCode uint16, val uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], val)
	m := &AckMsg{ResultCode: resultCode, Data: b[:]}
	return m.Encode()
}

// DecodeAckFD extracts a file descriptor from already-decoded AckMsg.Data.
func DecodeAckFD(data []byte) (uint16, uint32) {
	if len(data) < 4 {
		return 0, 0
	}
	return 0, binary.BigEndian.Uint32(data)
}

// DecodeAckU32 extracts a uint32 from already-decoded AckMsg.Data.
func DecodeAckU32(data []byte) (uint16, uint32) {
	return DecodeAckFD(data)
}

// EncodeError creates an ErrorMsg payload.
func EncodeError(code uint16, message string) []byte {
	m := &ErrorMsg{Code: code, Message: message}
	return m.Encode()
}
