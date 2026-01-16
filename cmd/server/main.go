package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jdpolicano/go-search/internal/logging"
	"github.com/jdpolicano/go-search/internal/server"
	"github.com/jdpolicano/go-search/internal/store"
)

func main() {
	logger := logging.NewLogger(slog.LevelInfo)

	s, err := store.NewStore("db/store.db")
	if err != nil {
		logger.Error("Error creating store", "error", err)
		os.Exit(1)
	}
	defer s.Pool.Close()

	srv := server.NewServer(s, logger)

	serverCtx, serverCancel := context.WithCancel(context.Background())
	defer serverCancel()

	go func() {
		logger.Info("Starting search server on :8080...")
		if err := srv.Start(serverCtx); err != nil && err != http.ErrServerClosed {
			logger.Error("Server error", "error", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	logger.Info("Received signal, shutting down gracefully", "signal", sig)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("Server shutdown error", "error", err)
		os.Exit(1)
	}

	logger.Info("Server stopped gracefully")
}
