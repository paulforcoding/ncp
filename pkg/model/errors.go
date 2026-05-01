package model

// Error codes: 0x0001-0x0FFF general, 0x1000-0x1FFF file,
// 0x2000-0x2FFF protocol, 0x3000-0xFFFF reserved.
const (
	// General errors (0x0001-0x0FFF)
	ErrUnknown          uint16 = 0x0001
	ErrInvalidArgument  uint16 = 0x0002
	ErrConfig           uint16 = 0x0003
	ErrCredential       uint16 = 0x0004
	ErrTaskNotFound     uint16 = 0x0005
	ErrTaskRunning      uint16 = 0x0006
	ErrWalkIncomplete   uint16 = 0x0007

	// File errors (0x1000-0x1FFF)
	ErrFileOpen         uint16 = 0x1001
	ErrFileRead         uint16 = 0x1002
	ErrFileWrite        uint16 = 0x1003
	ErrFileMkdir        uint16 = 0x1004
	ErrFileSymlink      uint16 = 0x1005
	ErrFileMetadata     uint16 = 0x1006
	ErrChecksumMismatch uint16 = 0x1007
	ErrFilePermission   uint16 = 0x1008

	// Protocol errors (0x2000-0x2FFF)
	ErrProtocol         uint16 = 0x2001
	ErrProtocolVersion  uint16 = 0x2002
	ErrProtocolAuth     uint16 = 0x2003
	ErrConnectionLost   uint16 = 0x2004
)
