package config

import (
	"errors"
	"os"
	"strings"
)

type Config struct {
	Mode, Addr, MetricsAddr, DatabaseURL, Topic, Group, AdminToken, SigningKey, WebDir string
	Brokers                                                                            []string
	SecureCookie                                                                       bool
	KafkaTLS                                                                           bool
	KafkaUser, KafkaPassword                                                           string
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func Load() (Config, error) {
	c := Config{Mode: env("MODE", "api"), Addr: env("ADDR", ":8080"), MetricsAddr: env("METRICS_ADDR", ":9090"), DatabaseURL: os.Getenv("DATABASE_URL"), Topic: env("KAFKA_TOPIC", "votes.v1"), Group: env("KAFKA_GROUP", "voting-projector-v1"), Brokers: strings.Split(env("KAFKA_BROKERS", "localhost:19092"), ","), AdminToken: os.Getenv("ADMIN_TOKEN"), SigningKey: os.Getenv("SIGNING_KEY"), WebDir: env("WEB_DIR", "web/dist"), SecureCookie: env("COOKIE_SECURE", "true") == "true", KafkaTLS: env("KAFKA_TLS", "false") == "true", KafkaUser: os.Getenv("KAFKA_USER"), KafkaPassword: os.Getenv("KAFKA_PASSWORD")}
	if c.DatabaseURL == "" {
		return c, errors.New("DATABASE_URL is required")
	}
	if c.Mode == "api" && (len(c.AdminToken) < 24 || len(c.SigningKey) < 32) {
		return c, errors.New("ADMIN_TOKEN >=24 bytes and SIGNING_KEY >=32 bytes are required")
	}
	if c.Mode != "api" && c.Mode != "worker" && c.Mode != "migrate" {
		return c, errors.New("MODE must be api, worker or migrate")
	}
	return c, nil
}
