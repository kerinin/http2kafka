package main

type Queue struct {
	Mapping map[string]chan Response
}

func NewQueue() Queue {
	return Queue{make(map[string]chan Response)}
}

func (q *Queue) Enqueue(uuid string, c chan Response) {
	q.Mapping[uuid] = c
}

func (q *Queue) Dequeue(uuid string, m Response) {
	// Fetch Response and request channel
	c, ok := q.Mapping[uuid]

	if !ok {
		log.Error("Tried to respond to missing key", uuid)
		return
	}

	c <- m
	close(c)
}

func (q *Queue) Delete(uuid string) {
	delete(q.Mapping, uuid)
}
