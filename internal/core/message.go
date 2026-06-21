package core

type Message struct {
	Payload    []byte
	RoutingKey RoutingKey
}
