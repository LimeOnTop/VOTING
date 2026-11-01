package broker

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/LimeOnTop/VOTING/internal/config"
	"github.com/LimeOnTop/VOTING/internal/domain"
	"github.com/LimeOnTop/VOTING/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"log/slog"
	"sort"
	"time"
)

var Processed = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "voting_worker_events_total", Help: "Projection outcomes."}, []string{"outcome"})
var LastProcessed = prometheus.NewGauge(prometheus.GaugeOpts{Name: "voting_worker_last_processed_timestamp_seconds", Help: "Last successful projection wall clock time."})

func init() { prometheus.MustRegister(Processed, LastProcessed) }
func dialer(c config.Config) *kafka.Dialer {
	d := &kafka.Dialer{Timeout: 5 * time.Second, DualStack: true}
	if c.KafkaTLS {
		d.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if c.KafkaUser != "" {
		d.SASLMechanism = plain.Mechanism{Username: c.KafkaUser, Password: c.KafkaPassword}
	}
	return d
}

type Producer struct {
	Writer *kafka.Writer
	conf   config.Config
}

func New(c config.Config) *Producer {
	d := dialer(c)
	t := &kafka.Transport{Dial: d.DialFunc, TLS: d.TLS, SASL: d.SASLMechanism}
	return &Producer{Writer: &kafka.Writer{Addr: kafka.TCP(c.Brokers...), Topic: c.Topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, BatchTimeout: 5 * time.Millisecond, BatchSize: 1000, WriteTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, Transport: t, AllowAutoTopicCreation: false}, conf: c}
}
func (p *Producer) Publish(ctx context.Context, v domain.Vote) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return p.Writer.WriteMessages(ctx, kafka.Message{Key: []byte(v.PollID + ":" + v.VoterKey), Value: b, Time: v.AcceptedAt})
}
func (p *Producer) Ping(ctx context.Context) error {
	conn, err := dialer(p.conf).DialContext(ctx, "tcp", p.conf.Brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	}
	_, err = conn.ReadPartitions(p.conf.Topic)
	return err
}
func ValidateEvent(ctx context.Context, s *store.Store, v domain.Vote) error {
	if v.Version != 1 {
		return errors.New("unsupported vote version")
	}
	key, err := hex.DecodeString(v.VoterKey)
	if err != nil || len(key) != 32 {
		return errors.New("invalid voter key")
	}
	p, err := s.Get(ctx, v.PollID)
	if err != nil {
		return err
	}
	if err = p.ValidateVote(v.Options, v.AcceptedAt); err != nil {
		return err
	}
	if v.AcceptedAt.After(time.Now().Add(time.Minute)) {
		return errors.New("future event")
	}
	return nil
}

// Offsets are committed only after the transaction. Replay after a crash is harmless
// because the unique ballot and its counters commit together in PostgreSQL.
func Run(ctx context.Context, c config.Config, s *store.Store) error {
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: c.Brokers, Topic: c.Topic, GroupID: c.Group, Dialer: dialer(c), MinBytes: 1, MaxBytes: 10e6, CommitInterval: 0, StartOffset: kafka.FirstOffset, MaxWait: 250 * time.Millisecond})
	defer r.Close()
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			return err
		}
		var v domain.Vote
		if err = json.Unmarshal(m.Value, &v); err != nil {
			return errors.New("invalid event JSON: refusing to advance offset")
		}
		// Fail closed on malformed events. Operator repairs/replays rather than silently losing a ballot.
		if err = ValidateEvent(ctx, s, v); err != nil {
			return err
		}
		sort.Strings(v.Options)
		for {
			applied, e := s.Apply(ctx, v)
			if e == nil {
				if applied {
					Processed.WithLabelValues("counted").Inc()
				} else {
					Processed.WithLabelValues("duplicate").Inc()
				}
				LastProcessed.Set(float64(time.Now().Unix()))
				break
			}
			Processed.WithLabelValues("retry").Inc()
			slog.Error("projection retry", "error", e)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
		if err = r.CommitMessages(ctx, m); err != nil {
			return err
		}
	}
}
