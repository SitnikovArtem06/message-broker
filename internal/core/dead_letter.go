package core

import "sync"

type DeadLetter struct {
	Message     Message
	SourceQueue string
	Reason      string
	Attempts    int
}

type DeadLetterQueue struct {
	mu    sync.Mutex
	Items []DeadLetter
}

func (q *DeadLetterQueue) Append(letter DeadLetter) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.Items = append(q.Items, letter)
}
