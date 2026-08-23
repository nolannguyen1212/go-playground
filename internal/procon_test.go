package internal_test

import (
	"testing"

	"github.com/go-jose/go-jose/v4/testutils/assert"
	"github.com/nolannguyen1212/go-runbook/internal"
)

func TestProducer(t *testing.T) {
	ch := make(chan int, 64)

	go internal.Produce(3, ch)

	wants := []int{0, 3, 6, 9, 12}
	for _, expected := range wants {
		got := <-ch
		assert.Equal(t, got, expected)
	}
}
