package core

type Delivery struct {
    ID         string
    Message    Message
    ConsumerID string
    Attempts   int
}
