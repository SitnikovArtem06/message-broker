package core

type Message struct {
	//ID         string
	Payload    []byte
	RoutingKey RoutingKey
}
