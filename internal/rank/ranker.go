package rank

import (
	"context"
	"log/slog"
	"math"
	"time"

	"github.com/jdpolicano/go-search/internal/store"
)

type Ranker struct {
	store      store.Store
	logger     *slog.Logger
	interval   time.Duration
	maxRetries int
	baseDelay  time.Duration
}

func NewRanker(store store.Store, logger *slog.Logger, interval time.Duration) *Ranker {
	return &Ranker{
		store:      store,
		logger:     logger,
		interval:   interval,
		maxRetries: 5,
		baseDelay:  100 * time.Millisecond,
	}
}

func (r *Ranker) retryWithBackoff(ctx context.Context, phase string, operation func(context.Context) error) error {
	var lastErr error

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(float64(r.baseDelay) * math.Pow(2, float64(attempt-1)))
			delay = min(delay, 5*time.Second) // Cap at 5 seconds

			r.logger.Warn("Retrying ranking phase after error",
				"phase", phase,
				"attempt", attempt,
				"maxRetries", r.maxRetries,
				"delay", delay,
				"lastError", lastErr)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// Continue with retry
			}
		}

		if err := operation(ctx); err != nil {
			lastErr = err
			if attempt < r.maxRetries {
				r.logger.Error("Ranking phase failed",
					"phase", phase,
					"attempt", attempt+1,
					"error", err)
				continue
			}
		} else {
			if attempt > 0 {
				r.logger.Info("Ranking phase succeeded on retry",
					"phase", phase,
					"attempt", attempt+1)
			}
			return nil
		}
	}

	return lastErr
}

func (r *Ranker) Start(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.logger.Info("Running initial ranking update...")
	if err := r.updateRankings(ctx); err != nil {
		r.logger.Error("Initial ranking update failed", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Ranker stopped")
			return ctx.Err()
		case <-ticker.C:
			r.logger.Info("Running scheduled ranking update...")
			if err := r.updateRankings(ctx); err != nil {
				r.logger.Error("Scheduled ranking update failed", "error", err)
			}
		}
	}
}

func (r *Ranker) updateRankings(ctx context.Context) error {
	start := time.Now()

	r.logger.Info("Phase 1: Updating document frequencies...")
	if err := r.retryWithBackoff(ctx, "document_frequency", func(ctx context.Context) error {
		return store.UpdateDocumentFrequency(ctx, r.store.Pool)
	}); err != nil {
		return err
	}

	r.logger.Info("Phase 2: Updating inverse document frequencies...")
	if err := r.retryWithBackoff(ctx, "inverse_document_frequency", func(ctx context.Context) error {
		return store.UpdateInverseDocumentFrequency(ctx, r.store.Pool)
	}); err != nil {
		return err
	}

	r.logger.Info("Phase 3: Updating document norms...")
	if err := r.retryWithBackoff(ctx, "document_norms", func(ctx context.Context) error {
		return store.UpdateDocumentNorms(ctx, r.store.Pool)
	}); err != nil {
		return err
	}

	duration := time.Since(start)
	r.logger.Info("Ranking update completed", "duration", duration)
	return nil
}
