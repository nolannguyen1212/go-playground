package internal

import (
	"sync"
)

type (
	Topic      string
	Subscriber chan any
)

const (
	TopicA Topic = "A"
	TopicB Topic = "B"
)

type Broker struct {
	mu          sync.Mutex
	subscribers map[Subscriber]Topic
}

func NewBroker() *Broker {
	return &Broker{
		subscribers: make(map[Subscriber]Topic),
	}
}

func (b *Broker) Subscribe(topic Topic) Subscriber {
	b.mu.Lock()
	defer b.mu.Unlock()

	sub := make(Subscriber, 1)
	b.subscribers[sub] = topic

	return sub
}

func (b *Broker) Unsubscribe(sub Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.subscribers[sub]; !ok {
		return
	}

	delete(b.subscribers, sub)
	close(sub)
}

func (b *Broker) Publish(topic Topic, msg any) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for sub, t := range b.subscribers {
		if t == topic {
			sub <- msg
		}
	}
}

func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for sub := range b.subscribers {
		delete(b.subscribers, sub)
		close(sub)
	}
}
