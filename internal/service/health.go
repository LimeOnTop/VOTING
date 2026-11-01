package service

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/LimeOnTop/VOTING/internal/usecase"
)

type HealthOptions struct{ Interval, Timeout time.Duration }
type HealthService struct {
	checks  []usecase.HealthCheck
	ready   atomic.Bool
	options HealthOptions
}

var _ usecase.HealthStatus = (*HealthService)(nil)

func NewHealthService(checks []usecase.HealthCheck, options HealthOptions) (*HealthService, error) {
	if len(checks) == 0 || options.Interval <= 0 || options.Timeout <= 0 {
		return nil, fmt.Errorf("invalid health checks or options")
	}
	for _, check := range checks {
		if check == nil {
			return nil, fmt.Errorf("nil health check")
		}
	}
	return &HealthService{checks: checks, options: options}, nil
}
func (h *HealthService) Ready() bool { return h.ready.Load() }
func (h *HealthService) Stop()       { h.ready.Store(false) }
func (h *HealthService) Run(ctx context.Context) {
	ticker := time.NewTicker(h.options.Interval)
	defer ticker.Stop()
	defer h.Stop()
	for {
		bounded, cancel := context.WithTimeout(ctx, h.options.Timeout)
		healthy := true
		for _, check := range h.checks {
			if err := check.Ping(bounded); err != nil {
				healthy = false
				break
			}
		}
		cancel()
		if ctx.Err() != nil {
			return
		}
		h.ready.Store(healthy)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
