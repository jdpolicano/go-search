package logging

import (
	"context"
	"log/slog"
	"os"
)

type contextKey string

const (
	CorrelationIDKey contextKey = "correlation_id"
)

var defaultLogger *slog.Logger

func init() {
	defaultLogger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(defaultLogger)
}

func NewLogger(level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
}

func WithCorrelationID(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, CorrelationIDKey, correlationID)
}

func GetCorrelationID(ctx context.Context) string {
	if id, ok := ctx.Value(CorrelationIDKey).(string); ok {
		return id
	}
	return ""
}

func WithContext(logger *slog.Logger, ctx context.Context) *slog.Logger {
	if correlationID := GetCorrelationID(ctx); correlationID != "" {
		return logger.With("correlation_id", correlationID)
	}
	return logger
}

func Default() *slog.Logger {
	return defaultLogger
}

func SetLevel(level slog.Level) {
	defaultLogger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
	slog.SetDefault(defaultLogger)
}
