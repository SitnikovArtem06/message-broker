package core

type Delivery struct {
	ID         string
	Message    Message
	ConsumerID string
	Attempts   int
}

type PublishedDelivery struct {
	QueueName string
	Queue     *Queue
	Delivery  Delivery
}
