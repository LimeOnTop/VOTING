// Package client implements external message-broker adapters.
package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/LimeOnTop/VOTING/internal/entity"
	"github.com/LimeOnTop/VOTING/internal/usecase"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type KafkaOptions struct {
	Brokers                                []string
	Topic, Group                           string
	TLS                                    bool
	Username, Password                     string
	DialTimeout, ReadTimeout, WriteTimeout time.Duration
	ProducerBatchWait, ConsumerBatchWait   time.Duration
	ProducerBatchSize, ConsumerBatchSize   int
	MinBytes, MaxBytes                     int
}

func (o KafkaOptions) validate(consumer bool) error {
	if len(o.Brokers) == 0 {
		return errors.New("kafka brokers are required")
	}
	for _, addr := range o.Brokers {
		host, port, err := net.SplitHostPort(addr)
		if err != nil || host == "" || port == "" || strings.ContainsAny(host, "@ /\t\r\n") {
			return errors.New("kafka brokers must be host:port addresses")
		}
	}
	if o.Topic == "" || len(o.Topic) > 249 || o.Topic == "." || o.Topic == ".." || strings.ContainsAny(o.Topic, " /\t\r\n") {
		return errors.New("invalid Kafka topic")
	}
	for _, r := range o.Topic {
		validCharacter := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-'
		if !validCharacter {
			return errors.New("invalid Kafka topic characters")
		}
	}
	if o.DialTimeout <= 0 || o.ReadTimeout <= 0 || o.WriteTimeout <= 0 {
		return errors.New("kafka deadlines must be positive")
	}
	if (o.Username == "") != (o.Password == "") {
		return errors.New("kafka SASL username and password must be supplied together")
	}
	if o.Username != "" && !o.TLS {
		return errors.New("kafka SASL PLAIN requires TLS")
	}
	if consumer {
		if o.Group == "" {
			return errors.New("kafka consumer group is required")
		}
		if o.ConsumerBatchSize <= 0 || o.ConsumerBatchWait <= 0 {
			return errors.New("kafka consumer batch size and wait must be positive")
		}
		if o.MinBytes <= 0 || o.MaxBytes < o.MinBytes {
			return errors.New("invalid Kafka fetch byte bounds")
		}
	} else if o.ProducerBatchSize <= 0 || o.ProducerBatchWait <= 0 {
		return errors.New("kafka producer batch size and wait must be positive")
	}
	return nil
}

func transport(o KafkaOptions) *kafka.Transport {
	t := &kafka.Transport{DialTimeout: o.DialTimeout, MetadataTopics: []string{o.Topic}}
	if o.TLS {
		t.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if o.Username != "" {
		t.SASL = plain.Mechanism{Username: o.Username, Password: o.Password}
	}
	return t
}
func dialer(o KafkaOptions) *kafka.Dialer {
	d := &kafka.Dialer{Timeout: o.DialTimeout, DualStack: true}
	if o.TLS {
		d.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if o.Username != "" {
		d.SASLMechanism = plain.Mechanism{Username: o.Username, Password: o.Password}
	}
	return d
}

type messageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

// VotePublisher waits for durable ACK; the worker handles duplicate delivery.
type VotePublisher struct {
	writer    messageWriter
	health    *kafkaHealth
	transport *kafka.Transport
}

var _ usecase.VotePublisher = (*VotePublisher)(nil)
var _ usecase.HealthCheck = (*VotePublisher)(nil)

func NewVotePublisher(options KafkaOptions) (*VotePublisher, error) {
	if err := options.validate(false); err != nil {
		return nil, fmt.Errorf("configure Kafka publisher: %w", err)
	}
	options.Brokers = append([]string(nil), options.Brokers...)
	t := transport(options)
	w := &kafka.Writer{Addr: kafka.TCP(options.Brokers...), Topic: options.Topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, Async: false, AllowAutoTopicCreation: false, BatchSize: options.ProducerBatchSize, BatchTimeout: options.ProducerBatchWait, ReadTimeout: options.ReadTimeout, WriteTimeout: options.WriteTimeout, Transport: t}
	return &VotePublisher{writer: w, health: newHealth(options, t), transport: t}, nil
}
func (p *VotePublisher) Publish(ctx context.Context, v entity.Vote) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode vote event: %w", err)
	}
	if err = p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(v.PollID + ":" + v.VoterKey), Value: data, Time: v.AcceptedAt}); err != nil {
		return fmt.Errorf("publish vote event: %w", err)
	}
	return nil
}
func (p *VotePublisher) Ping(ctx context.Context) error {
	if err := p.health.Ping(ctx); err != nil {
		return fmt.Errorf("ping vote publisher: %w", err)
	}
	return nil
}
func (p *VotePublisher) Close() error {
	err := p.writer.Close()
	if p.transport != nil {
		p.transport.CloseIdleConnections()
	}
	if err != nil {
		return fmt.Errorf("close vote publisher: %w", err)
	}
	return nil
}

type messageReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}

// VoteConsumer leaves positions uncommitted until all SQL shards succeed.
type VoteConsumer struct {
	reader    messageReader
	options   KafkaOptions
	health    *kafkaHealth
	transport *kafka.Transport
}

var _ usecase.VoteConsumer = (*VoteConsumer)(nil)
var _ usecase.HealthCheck = (*VoteConsumer)(nil)

func NewVoteConsumer(options KafkaOptions) (*VoteConsumer, error) {
	if err := options.validate(true); err != nil {
		return nil, fmt.Errorf("configure Kafka consumer: %w", err)
	}
	options.Brokers = append([]string(nil), options.Brokers...)
	cfg := kafka.ReaderConfig{Brokers: options.Brokers, Topic: options.Topic, GroupID: options.Group, Dialer: dialer(options), MinBytes: options.MinBytes, MaxBytes: options.MaxBytes, MaxWait: options.ConsumerBatchWait, ReadBatchTimeout: options.ReadTimeout, CommitInterval: 0, StartOffset: kafka.FirstOffset}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("kafka reader configuration: %w", err)
	}
	t := transport(options)
	return &VoteConsumer{reader: kafka.NewReader(cfg), options: options, health: newHealth(options, t), transport: t}, nil
}

func (c *VoteConsumer) FetchBatch(ctx context.Context) ([]usecase.Delivery, error) {
	first, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("read vote delivery: %w", err)
	}
	delivery, err := decode(first)
	if err != nil {
		return nil, fmt.Errorf("read vote delivery: %w", err)
	}
	batch := make([]usecase.Delivery, 0, c.options.ConsumerBatchSize)
	batch = append(batch, delivery)
	// Start the batch window after the first message.
	window, cancel := context.WithTimeout(ctx, c.options.ConsumerBatchWait)
	defer cancel()
	for len(batch) < c.options.ConsumerBatchSize {
		m, err := c.reader.FetchMessage(window)
		if err != nil {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("read vote delivery: %w", ctx.Err())
			}
			if errors.Is(err, context.DeadlineExceeded) && window.Err() != nil {
				return batch, nil
			}
			return nil, fmt.Errorf("fetch vote batch: %w", err)
		}
		d, err := decode(m)
		if err != nil {
			return nil, fmt.Errorf("read vote delivery: %w", err)
		}
		batch = append(batch, d)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("read vote delivery: %w", err)
	}
	return batch, nil
}
func decode(m kafka.Message) (usecase.Delivery, error) {
	var vote entity.Vote
	if err := json.Unmarshal(m.Value, &vote); err != nil {
		// Report only the cursor, not the ballot, identity or raw message data.
		return usecase.Delivery{}, fmt.Errorf("invalid vote JSON at topic %s partition %d offset %d: %w", m.Topic, m.Partition, m.Offset, err)
	}
	return usecase.Delivery{Vote: vote, Position: usecase.Position{Topic: m.Topic, Partition: m.Partition, Offset: m.Offset}}, nil
}
func (c *VoteConsumer) Commit(ctx context.Context, positions []usecase.Position) error {
	if len(positions) == 0 {
		return nil
	}
	// A maximum offset also acknowledges earlier messages in that partition.
	type partition struct {
		topic string
		id    int
	}
	maxima := make(map[partition]int64, len(positions))
	for _, pos := range positions {
		if pos.Topic != c.options.Topic || pos.Partition < 0 || pos.Offset < 0 {
			return errors.New("invalid consumer commit position")
		}
		key := partition{pos.Topic, pos.Partition}
		if previous, ok := maxima[key]; !ok || pos.Offset > previous {
			maxima[key] = pos.Offset
		}
	}
	messages := make([]kafka.Message, 0, len(maxima))
	for key, offset := range maxima {
		messages = append(messages, kafka.Message{Topic: key.topic, Partition: key.id, Offset: offset})
	}
	sort.Slice(messages, func(i, j int) bool {
		if messages[i].Topic != messages[j].Topic {
			return messages[i].Topic < messages[j].Topic
		}
		return messages[i].Partition < messages[j].Partition
	})
	if err := c.reader.CommitMessages(ctx, messages...); err != nil {
		return fmt.Errorf("commit vote batch positions: %w", err)
	}
	return nil
}
func (c *VoteConsumer) Ping(ctx context.Context) error {
	if err := c.health.Ping(ctx); err != nil {
		return fmt.Errorf("ping vote consumer: %w", err)
	}
	return nil
}
func (c *VoteConsumer) Close() error {
	err := c.reader.Close()
	if c.transport != nil {
		c.transport.CloseIdleConnections()
	}
	if err != nil {
		return fmt.Errorf("close vote consumer: %w", err)
	}
	return nil
}

type brokerProbe func(context.Context, string) error
type kafkaHealth struct {
	brokers []string
	timeout time.Duration
	probe   brokerProbe
}

func newHealth(o KafkaOptions, t *kafka.Transport) *kafkaHealth {
	client := &kafka.Client{Transport: t}
	return &kafkaHealth{brokers: o.Brokers, timeout: o.DialTimeout, probe: func(ctx context.Context, addr string) error {
		// Metadata must not request topic creation.
		result, err := client.Metadata(ctx, &kafka.MetadataRequest{Addr: kafka.TCP(addr), Topics: []string{o.Topic}})
		if err != nil {
			return fmt.Errorf("read Kafka metadata: %w", err)
		}
		for _, topic := range result.Topics {
			if topic.Name != o.Topic {
				continue
			}
			if topic.Error != nil {
				return fmt.Errorf("read Kafka topic metadata: %w", topic.Error)
			}
			if len(topic.Partitions) == 0 {
				return errors.New("kafka topic has no partitions")
			}
			for _, p := range topic.Partitions {
				if p.Error != nil {
					return fmt.Errorf("read Kafka partition metadata: %w", p.Error)
				}
				if p.Leader.Host == "" {
					return errors.New("kafka partition has no leader")
				}
			}
			return nil
		}
		return errors.New("kafka topic metadata unavailable")
	}}
}
func (h *kafkaHealth) Ping(ctx context.Context) error {
	deadline, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()
	failures := make([]error, 0, len(h.brokers))
	for i, addr := range h.brokers {
		if err := deadline.Err(); err != nil {
			return fmt.Errorf("kafka health deadline: %w", err)
		}
		end, _ := deadline.Deadline()
		budget := time.Until(end) / time.Duration(len(h.brokers)-i)
		attempt, stop := context.WithTimeout(deadline, budget)
		err := h.probe(attempt, addr)
		stop()
		if err == nil {
			return nil
		}
		failures = append(failures, err)
	}
	return fmt.Errorf("no Kafka bootstrap broker is available: %w", errors.Join(failures...))
}
