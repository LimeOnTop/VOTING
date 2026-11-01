package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LimeOnTop/VOTING/internal/entity"
	"github.com/LimeOnTop/VOTING/internal/usecase"
	"github.com/segmentio/kafka-go"
)

func kafkaOptions() KafkaOptions {
	return KafkaOptions{Brokers: []string{"localhost:19092"}, Topic: "votes.test", Group: "test-projector", DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, ProducerBatchWait: time.Millisecond, ConsumerBatchWait: 20 * time.Millisecond, ProducerBatchSize: 20, ConsumerBatchSize: 3, MinBytes: 1, MaxBytes: 1 << 20}
}

func TestKafkaOptionsRejectInvalidOrUnsafeConfiguration(t *testing.T) {
	cases := []struct {
		name   string
		change func(*KafkaOptions)
	}{
		{"no brokers", func(o *KafkaOptions) { o.Brokers = nil }},
		{"invalid broker", func(o *KafkaOptions) { o.Brokers = []string{"missing-port"} }},
		{"broker userinfo", func(o *KafkaOptions) { o.Brokers = []string{"secret@host:9092"} }},
		{"missing topic", func(o *KafkaOptions) { o.Topic = "" }},
		{"zero dial timeout", func(o *KafkaOptions) { o.DialTimeout = 0 }},
		{"missing SASL password", func(o *KafkaOptions) { o.TLS = true; o.Username = "user" }},
		{"PLAIN without TLS", func(o *KafkaOptions) { o.Username = "user"; o.Password = "not-sent" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := kafkaOptions()
			tc.change(&o)
			if _, err := NewVotePublisher(o); err == nil {
				t.Fatal("unsafe producer configuration accepted")
			}
			if _, err := NewVoteConsumer(o); err == nil {
				t.Fatal("unsafe consumer configuration accepted")
			}
		})
	}
	consumerCases := []struct {
		name   string
		change func(*KafkaOptions)
	}{
		{"no group", func(o *KafkaOptions) { o.Group = "" }},
		{"zero batch", func(o *KafkaOptions) { o.ConsumerBatchSize = 0 }},
		{"zero batch wait", func(o *KafkaOptions) { o.ConsumerBatchWait = 0 }},
		{"inverted fetch bounds", func(o *KafkaOptions) { o.MinBytes = o.MaxBytes + 1 }},
	}
	for _, tc := range consumerCases {
		t.Run(tc.name, func(t *testing.T) {
			o := kafkaOptions()
			tc.change(&o)
			if _, err := NewVoteConsumer(o); err == nil {
				t.Fatal("invalid consumer configuration accepted")
			}
		})
	}
	o := kafkaOptions()
	o.ProducerBatchSize = 0
	if _, err := NewVotePublisher(o); err == nil {
		t.Fatal("zero producer batch accepted")
	}
}

func TestProducerRequiresDurableSynchronousAcknowledgement(t *testing.T) {
	options := kafkaOptions()
	p, err := NewVotePublisher(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	w, ok := p.writer.(*kafka.Writer)
	if !ok {
		t.Fatal("unexpected writer adapter")
	}
	if w.RequiredAcks != kafka.RequireAll || w.Async || w.AllowAutoTopicCreation {
		t.Fatalf("unsafe writer settings: acks=%v async=%v autoCreate=%v", w.RequiredAcks, w.Async, w.AllowAutoTopicCreation)
	}
	if w.BatchSize != options.ProducerBatchSize || w.BatchTimeout != options.ProducerBatchWait {
		t.Fatal("producer batch configuration ignored")
	}
	// Constructors own a copy of bootstrap addresses.
	options.Brokers[0] = "changed:9092"
	if p.health.brokers[0] != "localhost:19092" {
		t.Fatal("caller can mutate constructed bootstrap list")
	}
}

func TestSASLUsesTLSWithoutCertificateBypass(t *testing.T) {
	o := kafkaOptions()
	o.TLS = true
	o.Username = "user"
	o.Password = "not-sent"
	p, err := NewVotePublisher(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	if p.transport.TLS == nil || p.transport.TLS.InsecureSkipVerify || p.transport.SASL == nil {
		t.Fatal("TLS/SASL not enforced")
	}
}

type writerStub struct {
	messages []kafka.Message
	err      error
}

func (w *writerStub) WriteMessages(_ context.Context, m ...kafka.Message) error {
	w.messages = append(w.messages, m...)
	return w.err
}
func (w *writerStub) Close() error { return nil }
func TestPublisherPreservesPartitionIdentityAndErrors(t *testing.T) {
	sink := &writerStub{}
	p := &VotePublisher{writer: sink}
	vote := entity.Vote{Version: 1, PollID: "poll", VoterKey: "browser-pseudonym", Options: []string{"1"}, AcceptedAt: time.Now().UTC()}
	if err := p.Publish(context.Background(), vote); err != nil {
		t.Fatal(err)
	}
	if len(sink.messages) != 1 || string(sink.messages[0].Key) != "poll:browser-pseudonym" {
		t.Fatal("incorrect partition key")
	}
	var decoded entity.Vote
	if err := json.Unmarshal(sink.messages[0].Value, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.PollID != vote.PollID || decoded.VoterKey != vote.VoterKey {
		t.Fatal("event changed")
	}
	down := errors.New("broker unavailable")
	sink.err = down
	if err := p.Publish(context.Background(), vote); !errors.Is(err, down) {
		t.Fatalf("publish error lost: %v", err)
	}
}

type readerStub struct {
	messages  []kafka.Message
	fetch     func(context.Context) (kafka.Message, error)
	commits   []kafka.Message
	commitErr error
}

func (r *readerStub) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if r.fetch != nil {
		return r.fetch(ctx)
	}
	if len(r.messages) > 0 {
		m := r.messages[0]
		r.messages = r.messages[1:]
		return m, nil
	}
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}
func (r *readerStub) CommitMessages(_ context.Context, m ...kafka.Message) error {
	r.commits = append(r.commits, m...)
	return r.commitErr
}
func (r *readerStub) Close() error { return nil }
func event(t *testing.T, partition int, offset int64) kafka.Message {
	t.Helper()
	b, err := json.Marshal(entity.Vote{Version: 1, PollID: "poll", VoterKey: fmt.Sprint(offset), Options: []string{"1"}, AcceptedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Topic: "votes.test", Partition: partition, Offset: offset, Value: b}
}

func TestConsumerFetchBatchDoesNotCommitBeforeService(t *testing.T) {
	r := &readerStub{messages: []kafka.Message{event(t, 0, 1), event(t, 1, 2), event(t, 0, 3)}}
	c := &VoteConsumer{reader: r, options: kafkaOptions()}
	got, err := c.FetchBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[1].Position.Partition != 1 || got[2].Position.Offset != 3 {
		t.Fatalf("incorrect batch: %+v", got)
	}
	if len(r.commits) != 0 {
		t.Fatal("fetch committed positions before projection")
	}
}
func TestConsumerFlushesPartialBatchAtWindowDeadline(t *testing.T) {
	r := &readerStub{messages: []kafka.Message{event(t, 0, 4)}}
	c := &VoteConsumer{reader: r, options: kafkaOptions()}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := c.FetchBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(r.commits) != 0 {
		t.Fatal("partial batch not returned safely")
	}
}
func TestConsumerCancellationAndMalformedEventNeverCommit(t *testing.T) {
	t.Run("cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		first := event(t, 0, 1)
		calls := 0
		r := &readerStub{}
		r.fetch = func(ctx context.Context) (kafka.Message, error) {
			calls++
			if calls == 1 {
				return first, nil
			}
			cancel()
			<-ctx.Done()
			return kafka.Message{}, ctx.Err()
		}
		c := &VoteConsumer{reader: r, options: kafkaOptions()}
		got, err := c.FetchBatch(ctx)
		if !errors.Is(err, context.Canceled) || got != nil || len(r.commits) != 0 {
			t.Fatalf("canceled batch advanced: %+v %v", got, err)
		}
	})
	t.Run("invalid JSON", func(t *testing.T) {
		payload := []byte(`{"private-payload": unquoted}`)
		r := &readerStub{messages: []kafka.Message{event(t, 0, 1), {Topic: "votes.test", Partition: 2, Offset: 9, Value: payload}}}
		c := &VoteConsumer{reader: r, options: kafkaOptions()}
		got, err := c.FetchBatch(context.Background())
		if err == nil || got != nil || len(r.commits) != 0 {
			t.Fatal("invalid event did not fail closed")
		}
		if !strings.Contains(err.Error(), "partition 2 offset 9") || strings.Contains(err.Error(), string(payload)) {
			t.Fatalf("missing cursor or leaked payload: %v", err)
		}
	})
}
func TestCommitUsesMaximumOffsetPerPartition(t *testing.T) {
	r := &readerStub{}
	c := &VoteConsumer{reader: r, options: kafkaOptions()}
	positions := []usecase.Position{{Topic: "votes.test", Partition: 1, Offset: 20}, {Topic: "votes.test", Partition: 0, Offset: 3}, {Topic: "votes.test", Partition: 1, Offset: 19}, {Topic: "votes.test", Partition: 0, Offset: 8}}
	if err := c.Commit(context.Background(), positions); err != nil {
		t.Fatal(err)
	}
	if len(r.commits) != 2 || r.commits[0].Partition != 0 || r.commits[0].Offset != 8 || r.commits[1].Partition != 1 || r.commits[1].Offset != 20 {
		t.Fatalf("incorrect maxima: %+v", r.commits)
	}
	down := errors.New("commit unavailable")
	r.commitErr = down
	if err := c.Commit(context.Background(), positions); !errors.Is(err, down) {
		t.Fatal("commit error lost")
	}
	before := len(r.commits)
	if err := c.Commit(context.Background(), []usecase.Position{{Topic: "another-topic", Partition: 0, Offset: 99}}); err == nil || len(r.commits) != before {
		t.Fatal("foreign topic position committed")
	}
}

func TestHealthTriesLaterBootstrapWithinOneDeadline(t *testing.T) {
	calls := []string{}
	h := &kafkaHealth{brokers: []string{"stalled:9092", "healthy:9092"}, timeout: 200 * time.Millisecond, probe: func(ctx context.Context, addr string) error {
		calls = append(calls, addr)
		if strings.HasPrefix(addr, "stalled") {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}}
	if err := h.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[1] != "healthy:9092" {
		t.Fatalf("bootstrap fallback missing: %v", calls)
	}
}
func TestHealthCancellationAndAllFailures(t *testing.T) {
	sentinel := errors.New("unreachable")
	h := &kafkaHealth{brokers: []string{"one:9092", "two:9092"}, timeout: time.Second, probe: func(context.Context, string) error { return sentinel }}
	if err := h.Ping(context.Background()); !errors.Is(err, sentinel) {
		t.Fatal("broker failures not returned")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.Ping(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}
func TestKafkaBootstrapFailoverIntegration(t *testing.T) {
	broker := os.Getenv("TEST_KAFKA_BROKER")
	if broker == "" {
		t.Skip("set TEST_KAFKA_BROKER for metadata-only Kafka integration")
	}
	options := kafkaOptions()
	options.Brokers = []string{"127.0.0.1:1", broker}
	options.Topic = os.Getenv("TEST_KAFKA_TOPIC")
	if options.Topic == "" {
		options.Topic = "votes.v1"
	}
	options.DialTimeout = 3 * time.Second
	p, err := NewVotePublisher(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := p.Ping(ctx); err != nil {
		t.Fatalf("healthy second bootstrap not used: %v", err)
	}
}
