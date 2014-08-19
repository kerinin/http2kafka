package main

type Waiter struct {
	channels []chan bool
}

func (w *Waiter) Channel() chan bool {
	c := make(chan bool)
	w.channels = append(w.channels, c)
	return c
}

func (w *Waiter) Wait() {
	for _, c := range w.channels {
		<-c
	}
}
