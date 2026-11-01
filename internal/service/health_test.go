package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LimeOnTop/VOTING/internal/usecase"
)

func waitReadiness(t *testing.T, service *HealthService, ready bool) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for service.Ready() != ready {
		select {
		case <-timer.C:
			t.Fatalf("readiness did not become %v", ready)
		case <-ticker.C:
		}
	}
}
func TestHealthStartsUnreadyAndCancellationCannotReenable(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	check := fakeCheck{ping: func(ctx context.Context) error {
		calls.Add(1)
		if _, ok := ctx.Deadline(); !ok {
			t.Error("health query has no timeout")
		}
		close(entered)
		<-release
		return nil
	}}
	service, err := NewHealthService([]usecase.HealthCheck{check}, HealthOptions{Interval: time.Hour, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if service.Ready() {
		t.Fatal("ready before checks ran")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { service.Run(ctx); close(done) }()
	<-entered
	cancel()
	service.Stop()
	close(release)
	<-done
	if service.Ready() {
		t.Fatal("late successful check reenabled readiness after shutdown")
	}
	if calls.Load() != 1 {
		t.Fatalf("checks continued after cancellation: %d", calls.Load())
	}
}
func TestHealthTracksFailureRecoveryAndStops(t *testing.T) {
	var healthy atomic.Bool
	check := fakeCheck{ping: func(context.Context) error {
		if !healthy.Load() {
			return errors.New("dependency unavailable")
		}
		return nil
	}}
	service, err := NewHealthService([]usecase.HealthCheck{check}, HealthOptions{Interval: time.Millisecond, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { service.Run(ctx); close(done) }()
	if service.Ready() {
		t.Fatal("failed dependency became ready")
	}
	healthy.Store(true)
	waitReadiness(t, service, true)
	healthy.Store(false)
	waitReadiness(t, service, false)
	cancel()
	<-done
	if service.Ready() {
		t.Fatal("ready after monitor stopped")
	}
}
