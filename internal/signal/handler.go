package signal

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// Setup creates a context that is cancelled on SIGINT or SIGTERM.
// The caller is responsible for propagating cancellation to all components.
func Setup() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-ch
		cancel()
	}()

	return ctx, func() {
		signal.Stop(ch)
		cancel()
	}
}
