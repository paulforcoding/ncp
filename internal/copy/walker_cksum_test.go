package copy

import (
	"testing"

	"github.com/zp001/ncp/pkg/model"
)

func TestShouldSkipForCopyResume(t *testing.T) {
	tests := []struct {
		name string
		cs   model.CopyStatus
		cks  model.CksumStatus
		want bool
	}{
		{"copy done + cksum pass", model.CopyDone, model.CksumPass, true},
		{"copy done + cksum none", model.CopyDone, model.CksumNone, true},
		{"copy done + cksum mismatch", model.CopyDone, model.CksumMismatch, false},
		{"copy done + cksum error", model.CopyDone, model.CksumError, false},
		{"copy done + cksum pending", model.CopyDone, model.CksumPending, false},
		{"copy error + cksum pass", model.CopyError, model.CksumPass, true},
		{"copy error + cksum none", model.CopyError, model.CksumNone, false},
		{"copy dispatched + cksum none", model.CopyDispatched, model.CksumNone, false},
		{"copy discovered + cksum none", model.CopyDiscovered, model.CksumNone, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldSkipForCopyResume(tt.cs, tt.cks)
			if got != tt.want {
				t.Errorf("shouldSkipForCopyResume(%v, %v) = %v, want %v", tt.cs, tt.cks, got, tt.want)
			}
		})
	}
}
