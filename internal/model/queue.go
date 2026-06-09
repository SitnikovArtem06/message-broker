package model

type Queue struct {
	Name         string
	Filters      []string
	IsDurable    bool
	IsAutoDelete bool
	Messages     *chan Message
}
