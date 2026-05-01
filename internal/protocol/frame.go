package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	Magic   uint32 = 0x4E435003 // "NCP\x03"
	Version uint8  = 1

	HeaderSize    = 10 // 4 (magic) + 1 (version) + 1 (type) + 4 (length)
	MaxPayloadSize = 16 * 1024 * 1024 // 16 MB
)

var (
	ErrBadMagic   = errors.New("protocol: bad magic")
	ErrBadVersion = errors.New("protocol: bad version")
	ErrTooLarge   = errors.New("protocol: payload too large")
)

// Frame represents a decoded protocol frame.
type Frame struct {
	Version uint8
	Type    uint8
	Payload []byte
}

// WriteFrame encodes and writes a frame to w.
func WriteFrame(w io.Writer, msgType uint8, payload []byte) error {
	if len(payload) > MaxPayloadSize {
		return ErrTooLarge
	}

	var hdr [HeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], Magic)
	hdr[4] = Version
	hdr[5] = msgType
	binary.BigEndian.PutUint32(hdr[6:10], uint32(len(payload)))

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

// ReadFrame reads and decodes a frame from r.
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
	if length > MaxPayloadSize {
		return nil, ErrTooLarge
	}

	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
	}

	return &Frame{Version: v, Type: msgType, Payload: payload}, nil
}
