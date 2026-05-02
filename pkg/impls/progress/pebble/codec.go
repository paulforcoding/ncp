package pebble

import "github.com/zp001/ncp/pkg/model"

// encodeValue packs copyStatus and cksumStatus into a 2-byte slice.
func encodeValue(cs model.CopyStatus, cks model.CksumStatus) []byte {
	return []byte{byte(cs), byte(cks)}
}

// decodeValue unpacks a byte slice into copyStatus and cksumStatus.
func decodeValue(val []byte) (model.CopyStatus, model.CksumStatus) {
	if len(val) == 0 {
		return model.CopyDiscovered, model.CksumNone
	}
	cs := model.CopyStatus(val[0])
	cks := model.CksumNone
	if len(val) > 1 {
		cks = model.CksumStatus(val[1])
	}
	return cs, cks
}
