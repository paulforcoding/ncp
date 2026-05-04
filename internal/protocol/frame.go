package protocol

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

const (
	Magic   uint32 = 0x4E435004 // "NCP\x04" — bumped for CRC32
	Version uint8  = 2

	HeaderSize     = 14               // 4 (magic) + 1 (version) + 1 (type) + 4 (length) + 4 (crc32)
	MaxPayloadSize = 16 * 1024 * 1024 // 16 MB
)

var (
	ErrBadMagic   = errors.New("protocol: bad magic")
	ErrBadVersion = errors.New("protocol: bad version")
	ErrTooLarge   = errors.New("protocol: payload too large")
	ErrCRC        = errors.New("protocol: CRC32 mismatch")
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Frame represents a decoded protocol frame.
type Frame struct {
	Version uint8
	Type    uint8
	Payload []byte
}

// WriteFrame encodes and writes a frame to w with a CRC32 checksum.
func WriteFrame(w io.Writer, msgType uint8, payload []byte) error {
	if len(payload) > MaxPayloadSize {
		return ErrTooLarge
	}

	crc := crc32.Checksum(payload, crcTable)

	var hdr [HeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], Magic)
	hdr[4] = Version
	hdr[5] = msgType
	binary.BigEndian.PutUint32(hdr[6:10], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[10:14], crc)

	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// ReadFrame reads and decodes a frame from r, verifying the CRC32 checksum.
func ReadFrame(r io.Reader) (*Frame, error) {
	var hdr [HeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}

	if m := binary.BigEndian.Uint32(hdr[0:4]); m != Magic {
		return nil, ErrBadMagic
	}
	v := hdr[4]
	if v != Version {
		return nil, ErrBadVersion
	}

	msgType := hdr[5]
	length := binary.BigEndian.Uint32(hdr[6:10])
	expectedCRC := binary.BigEndian.Uint32(hdr[10:14])
	if length > MaxPayloadSize {
		return nil, ErrTooLarge
	}

	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
	}

	if crc32.Checksum(payload, crcTable) != expectedCRC {
		return nil, ErrCRC
	}

	return &Frame{Version: v, Type: msgType, Payload: payload}, nil
}
