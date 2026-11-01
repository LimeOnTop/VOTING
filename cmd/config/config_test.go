package config

import (
	"strings"
	"testing"
)

func TestConfigurationValidation(t *testing.T) {
	cases := []struct {
		name, key, value string
		valid            bool
	}{
		{"defaults", "", "", true}, {"overflow", "DB_MAX_CONNECTIONS", "4294967297", false},
		{"submillisecond", "DB_STATEMENT_TIMEOUT", "1ns", false}, {"boolean typo", "COOKIE_SECURE", "tru", false},
		{"unprotected credentials", "KAFKA_USER", "user", false}, {"empty shards", "VOTE_DATABASE_URLS", "[]", false},
		{"bad shard JSON", "VOTE_DATABASE_URLS", "password-secret", false}, {"pool bounds", "DB_MIN_CONNECTIONS", "21", false},
		{"cache bounds", "CACHE_CAPACITY", "1", false}, {"worker needs no admin secrets", "MODE", "worker", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{"DATABASE_URL": "postgres://localhost/test", "ADMIN_TOKEN": strings.Repeat("a", 24), "SIGNING_KEY": strings.Repeat("b", 32)}
			if tc.key != "" {
				env[tc.key] = tc.value
			}
			if tc.value == "worker" {
				delete(env, "ADMIN_TOKEN")
				delete(env, "SIGNING_KEY")
			}
			_, err := LoadWithLookup(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
			if err != nil && strings.Contains(err.Error(), "password-secret") {
				t.Fatal("configuration leaked input")
			}
		})
	}
}
