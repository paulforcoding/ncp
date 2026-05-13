package ncpserver

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/pkg/impls/walkerdb/pebble"
	"github.com/zp001/ncp/pkg/interfaces/walkerdb"
	"github.com/zp001/ncp/pkg/model"
)

// taskWalker manages the background directory walk and paginated reads.
type taskWalker struct {
	db       walkerdb.Store
	dir      string
	basePath string
	count    int64
	done     bool
	doneCh   chan struct{}
	err      error
	mu       sync.Mutex
}

func newTaskWalker(taskID, basePath, tempDir string) *taskWalker {
	return &taskWalker{
		db:       &pebble.Store{},
		dir:      tempDir + "/walker-" + taskID,
		basePath: basePath,
		doneCh:   make(chan struct{}),
	}
}

// start opens the walker DB and launches the background walk goroutine.
func (tw *taskWalker) start() {
	if err := tw.db.Open(tw.dir); err != nil {
		tw.setError(err)
		close(tw.doneCh)
		return
	}
	go tw.backgroundWalk()
}

// backgroundWalk traverses the directory tree and writes entries to the walker DB.
func (tw *taskWalker) backgroundWalk() {
	defer close(tw.doneCh)

	seq := int64(0)
	walkErr := filepath.Walk(tw.basePath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		relPath, rerr := filepath.Rel(tw.basePath, path)
		if rerr != nil {
			return nil
		}
		relPath = filepath.ToSlash(relPath)
		if relPath == "." {
			return nil
		}

		mode := info.Mode()
		if mode&fs.ModeDevice != 0 || mode&fs.ModeNamedPipe != 0 || mode&fs.ModeSocket != 0 {
			return nil
		}

		entry := infoToListEntry(relPath, info, path)
		if perr := tw.db.Put(seq, entry); perr != nil {
			return fmt.Errorf("put entry at seq %d: %w", seq, perr)
		}
		seq++
		return nil
	})

	if walkErr != nil {
		tw.setError(walkErr)
		return
	}

	if err := tw.db.SetWalkComplete(); err != nil {
		tw.setError(err)
		return
	}

	tw.mu.Lock()
	tw.count = seq
	tw.done = true
	tw.mu.Unlock()
}

// waitForEntries reads entries starting from seq, up to limit.
// If the walk is ongoing and fewer than limit entries are available, it returns
// whatever is available with done=false. When walk is complete, done=true.
func (tw *taskWalker) waitForEntries(seq int64, limit int) (entries []walkerdb.Entry, done bool, err error) {
	// Check if walk is already done (fast path)
	tw.mu.Lock()
	isDone := tw.done
	walkErr := tw.err
	tw.mu.Unlock()

	if walkErr != nil {
		return nil, false, walkErr
	}

	entries, rerr := tw.db.GetRange(seq, limit)
	if rerr != nil {
		return nil, false, rerr
	}

	// If we got fewer than limit entries, check if walk is done
	if len(entries) < limit {
		if !isDone {
			// Walk is still ongoing — wait for it to make progress or complete
			select {
			case <-tw.doneCh:
				isDone = true
			default:
				// Try once more after a brief wait
			}
		}
		if isDone {
			// Walk is complete — try to get any remaining entries
			entries, rerr = tw.db.GetRange(seq, limit)
			if rerr != nil {
				return nil, false, rerr
			}
		}
	}

	return entries, isDone && len(entries) < limit, nil
}

// isDone reports whether the walk has completed.
func (tw *taskWalker) isDone() bool {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	return tw.done
}

// setError records a walk error.
func (tw *taskWalker) setError(err error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if tw.err == nil {
		tw.err = err
	}
}

// destroy waits for the background walk to complete naturally, then closes and removes the walker database.
func (tw *taskWalker) destroy() error {
	<-tw.doneCh

	if tw.db != nil {
		return tw.db.Destroy()
	}
	return nil
}

// infoToListEntry converts os.FileInfo to a protocol ListEntry.
func infoToListEntry(relPath string, info fs.FileInfo, fullPath string) protocol.ListEntry {
	mode := info.Mode()
	var ft uint8
	switch {
	case info.IsDir():
		ft = uint8(model.FileDir)
	case mode&fs.ModeSymlink != 0:
		ft = uint8(model.FileSymlink)
	default:
		ft = uint8(model.FileRegular)
	}

	entry := protocol.ListEntry{
		RelPath:  relPath,
		FileType: ft,
		FileSize: info.Size(),
		Mode:     uint32(mode.Perm()),
		Mtime:    info.ModTime().UnixNano(),
	}

	if mode&fs.ModeSymlink != 0 {
		if target, err := os.Readlink(fullPath); err == nil {
			entry.LinkTarget = target
		}
	}

	return entry
}
