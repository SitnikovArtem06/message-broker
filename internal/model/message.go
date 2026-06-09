package model

type Message struct {
	ID         string
	Payload    []byte
	RoutingKey string
}
