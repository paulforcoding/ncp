package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/zp001/ncp/internal/ncpserver"
)

// runServe handles `ncp serve` — starts the ncp protocol server.
func runServe(cmd *cobra.Command, args []string) error {
	listenAddr, _ := cmd.Flags().GetString("listen")

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", listenAddr, err)
	}

	srv := ncpserver.NewServer(listener)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		<-ctx.Done()
		srv.Shutdown()
	}()

	slog.Info("ncp serve started", "listen", listenAddr)

	err = srv.Serve()
	if err != nil {
		return err
	}

	// Serve returned (Shutdown was called), cleanup walker DB
	if cErr := srv.Cleanup(); cErr != nil {
		slog.Warn("server cleanup failed", "error", cErr)
	}
	return nil
}
