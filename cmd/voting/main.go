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

// @title VOTING API
// @version 1.0
// @description Анонимные опросы, управление публикациями и обезличенные результаты.
// @BasePath /api/v1
// @accept json
// @produce json
// @securityDefinitions.apikey AdminBearer
// @in header
// @name Authorization
// @description Bearer <ADMIN_TOKEN>.
// @securityDefinitions.apikey BrowserCookie
// @in header
// @name Cookie
// @description Подписанная cookie voting_id, полученная через GET /api/v1/session. Браузер передаёт её автоматически.
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
