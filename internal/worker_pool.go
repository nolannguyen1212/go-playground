package internal

import "fmt"

type Worker struct {
	id    int
	queue chan int
	done  chan bool
}

func NewWorker(id int, queue chan int, done chan bool) *Worker {
	return &Worker{
		id:    id,
		queue: queue,
		done:  done,
	}
}

func (w *Worker) Run() {
	for j := range w.queue {
		fmt.Println("worker", w.id, "finished job", j)
		w.done <- true
	}
}
