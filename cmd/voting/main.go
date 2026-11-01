package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/LimeOnTop/VOTING/cmd/config"
	"github.com/LimeOnTop/VOTING/internal/app"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration rejected", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err = app.Run(ctx, cfg, logger); err != nil {
		logger.Error("service stopped", "error", err)
		os.Exit(1)
	}
}
