package model

type Exchange struct {
	Name   string
	Queues map[string]*Queue
}
