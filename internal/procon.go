package internal

import "fmt"

func Produce(factor int, out chan<- int) {
	for i := range 5 {
		out <- i * factor
	}
}

func Consume(in <-chan int) {
	for v := range in {
		fmt.Println(v)
	}
}
