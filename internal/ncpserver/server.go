package ncpserver

import (
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zp001/ncp/internal/protocol"
	"github.com/zp001/ncp/pkg/model"
)

const (
	ServerModeUninitialized  uint8 = 0
	ServerModeSource         uint8 = 1
	ServerModeDestination    uint8 = 2
	ServerModeSourceNoWalker uint8 = 3
)

// Server listens for ncp protocol connections and serves a single task.
// It accepts multiple connections for the same taskID (for resume/reconnect),
// but rejects different taskIDs or modes.
type Server struct {
	listener    net.Listener
	cfg         *model.ServerConfig // parsed from first InitMsg
	walker      *taskWalker
	mu          sync.Mutex
	mode        uint8
	taskID      string
	activeConns int64
	quit        chan struct{}
}

// NewServer creates a Server bound to the given listener.
func NewServer(listener net.Listener) *Server {
	return &Server{
		listener: listener,
		quit:     make(chan struct{}),
	}
}

// ApplyConfig stores the ServerConfig received from the first client connection.
func (s *Server) ApplyConfig(cfg *model.ServerConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg = cfg
}

// TempDir returns the directory for walker DB storage.
// Uses ProgressStorePath from ServerConfig, or /tmp/ncpserve as default.
func (s *Server) TempDir() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg != nil && s.cfg.ProgressStorePath != "" {
		return s.cfg.ProgressStorePath
	}
	return "/tmp/ncpserve"
}

// Serve accepts connections and dispatches each to a ConnHandler.
// Blocks until Shutdown is called or an unrecoverable Accept error occurs.
func (s *Server) Serve() error {
	for {
		netConn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				// Graceful shutdown: wait for all handlers to finish
				for atomic.LoadInt64(&s.activeConns) > 0 {
					time.Sleep(10 * time.Millisecond)
				}
				return nil
			default:
				return err
			}
		}

		atomic.AddInt64(&s.activeConns, 1)
		go func() {
			defer atomic.AddInt64(&s.activeConns, -1)
			conn := protocol.NewConn(netConn)
			defer conn.Close()

			handler := NewConnHandler(s)
			if err := handler.HandleConn(conn); err != nil {
				slog.Debug("handler exited", "remote", conn.RemoteAddr(), "err", err)
			}
		}()
	}
}

// Shutdown closes the listener and signals the serve loop to exit.
// It does not wait for handlers to finish; Serve() does that.
func (s *Server) Shutdown() {
	select {
	case <-s.quit:
		return // already shutting down
	default:
		close(s.quit)
		s.listener.Close()
	}
}

// Cleanup closes and removes the walker database.
// Called after Serve() returns to clean up temp resources.
func (s *Server) Cleanup() error {
	if s.walker != nil {
		return s.walker.destroy()
	}
	return nil
}

// Addr returns the listener's address.
func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

// Mode returns the current server mode (safe for concurrent read).
func (s *Server) Mode() uint8 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mode
}

// TaskID returns the current task ID (safe for concurrent read).
func (s *Server) TaskID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.taskID
}
