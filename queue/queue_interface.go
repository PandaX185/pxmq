package queue

type Queue interface {
	Enqueue(msg []byte) error
	Dequeue() ([]byte, error)
}

var queues = make(map[string]Queue)
