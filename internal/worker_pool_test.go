package internal_test

import (
	"testing"

	"github.com/go-jose/go-jose/v4/testutils/assert"
	"github.com/nolannguyen1212/go-runbook/internal"
)

func TestWorkerPool(t *testing.T) {
	numberOfWorkers := 4
	numberOfJobs := 10

	q := make(chan int)
	done := make(chan bool)

	for i := range numberOfWorkers {
		w := internal.NewWorker(i, q, done)
		go w.Run()
	}

	for j := range numberOfJobs {
		go func() {
			q <- j
		}()
	}

	for range numberOfJobs {
		assert.Equal(t, <-done, true)
	}
}
