package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jdpolicano/go-search/internal/logging"
	"github.com/jdpolicano/go-search/internal/rank"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ranker := rank.NewRanker(s, logger, 10*time.Minute)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Info("Received signal, shutting down gracefully", "signal", sig)
		cancel()
	}()

	logger.Info("Starting ranking update service...")
	if err := ranker.Start(ctx); err != nil {
		logger.Error("Ranking service error", "error", err)
		os.Exit(1)
	}

	logger.Info("Ranking service stopped")
}
