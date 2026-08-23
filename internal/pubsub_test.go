package internal_test

import (
	"testing"

	"github.com/go-jose/go-jose/v4/testutils/assert"
	"github.com/nolannguyen1212/go-playground/internal"
)

func newBroker(t *testing.T) *internal.Broker {
	t.Helper()

	broker := internal.NewBroker()
	t.Cleanup(broker.Close)

	return broker
}

func TestPublishByTopic(t *testing.T) {
	broker := newBroker(t)

	a := broker.Subscribe(internal.TopicA)
	b := broker.Subscribe(internal.TopicB)

	broker.Publish(internal.TopicA, "hello A")
	assert.Equal(t, <-a, "hello A")

	select {
	case msg := <-b:
		t.Fatalf("b should receive nothing, got %v", msg)
	default:
	}
}

func TestUnsubscribe(t *testing.T) {
	broker := newBroker(t)

	sub := broker.Subscribe(internal.TopicA)
	broker.Unsubscribe(sub)

	_, ok := <-sub
	assert.Equal(t, ok, false)
}
