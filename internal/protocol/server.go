package protocol

import (
	"log/slog"
	"net"
	"sync"
)

// ConnHandler handles all messages on a single connection.
// The server creates a new ConnHandler for each accepted connection.
type ConnHandler interface {
	HandleConn(conn *Conn) error
}

// ConnHandlerFunc is a convenience adapter for ConnHandler.
type ConnHandlerFunc func(conn *Conn) error

func (f ConnHandlerFunc) HandleConn(conn *Conn) error { return f(conn) }

// Server listens for ncp protocol connections and dispatches each to a ConnHandler.
type Server struct {
	listener   net.Listener
	newHandler func() ConnHandler
	wg         sync.WaitGroup
	quit       chan struct{}
}

// NewServer creates a Server that dispatches each connection to a new ConnHandler
// created by the newHandler factory.
func NewServer(listener net.Listener, newHandler func() ConnHandler) *Server {
	return &Server{
		listener:   listener,
		newHandler: newHandler,
		quit:       make(chan struct{}),
	}
}

// Serve accepts connections and spawns a goroutine per connection.
// Blocks until the listener is closed or an Accept error occurs.
func (s *Server) Serve() error {
	for {
		netConn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return nil // graceful shutdown
			default:
				return err
			}
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			conn := NewConn(netConn)
			defer conn.Close()

			handler := s.newHandler()
			if err := handler.HandleConn(conn); err != nil {
				slog.Debug("connection handler exited", "remote", conn.RemoteAddr(), "err", err)
			}
		}()
	}
}

// Close gracefully shuts down the server.
func (s *Server) Close() error {
	close(s.quit)
	err := s.listener.Close()
	s.wg.Wait()
	return err
}
