package app

import (
	"errors"
	"strings"
	"testing"

	"github.com/LimeOnTop/VOTING/cmd/config"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestDatabaseConfigurationErrorRetainsCauseWithoutCredentials(t *testing.T) {
	_, err := openPool(t.Context(), "postgres://review-user:private-password@%invalid/database", config.Database{})
	if err == nil {
		t.Fatal("invalid DSN accepted")
	}
	var parseErr *pgconn.ParseConfigError
	if !errors.As(err, &parseErr) {
		t.Fatalf("database cause lost: %v", err)
	}
	if strings.Contains(err.Error(), "private-password") || strings.Contains(err.Error(), "review-user") {
		t.Fatal("credentials exposed through error string")
	}
}
