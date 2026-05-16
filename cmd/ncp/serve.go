package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/zp001/ncp/internal/di"
	"github.com/zp001/ncp/internal/ncpserver"
	"github.com/zp001/ncp/internal/protocol"
)

// runServe handles `ncp serve` — starts the ncp protocol server.
func runServe(cmd *cobra.Command, args []string) error {
	listenAddr, _ := cmd.Flags().GetString("listen")
	tempDir, _ := cmd.Flags().GetString("serve-temp-dir")

	if err := ncpserver.CleanupTempDir(tempDir); err != nil {
		return fmt.Errorf("cleanup temp dir: %w", err)
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", listenAddr, err)
	}

	srv := ncpserver.NewServer(listener, tempDir)

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

// notifyRemoteTaskDone dials the remote ncp serve and sends MsgTaskDone
// after the task has completed. No-op if neither src nor dst is ncp://.
// srcMode is the mode used for remote sources (ModeSource or ModeSourceNoWalker).
func notifyRemoteTaskDone(srcBase, dstBase, taskID string, srcMode uint8) error {
	var addr, basePath string
	var mode uint8

	if u, err := di.ParsePath(srcBase); err == nil && u.Scheme == "ncp" {
		addr = u.Host
		basePath = u.Path
		mode = srcMode
	} else if u, err := di.ParsePath(dstBase); err == nil && u.Scheme == "ncp" {
		addr = u.Host
		basePath = u.Path
		mode = protocol.ModeDestination
	} else {
		return nil
	}

	conn, err := protocol.Dial(addr)
	if err != nil {
		return fmt.Errorf("dial remote for task done: %w", err)
	}
	defer conn.Close()

	initMsg := &protocol.InitMsg{BasePath: basePath, Mode: mode, TaskID: taskID}
	if _, err := conn.SendMsgRecvAck(protocol.MsgInit, initMsg.Encode()); err != nil {
		return fmt.Errorf("send init for task done: %w", err)
	}

	if _, err := conn.SendMsgRecvAck(protocol.MsgTaskDone, (&protocol.TaskDoneMsg{}).Encode()); err != nil {
		return fmt.Errorf("send task done: %w", err)
	}
	return nil
}
