package core

import (
	"slices"

	errs "github.com/SitnikovArtem06/message-broker/internal/errors"
)

type Queue struct {
	Name         string
	Filters      []RoutingFilter
	IsDurable    bool
	IsAutoDelete bool
	Messages     []Message
}


func (q *Queue) Fetch() (Message, error){
	if len(q.Messages) == 0{
		return Message{}, errs.QueueEmpty
	}

	msg := q.Messages[0]
	q.Messages = slices.Delete(q.Messages,0,1)
	return msg, nil
}

func (q *Queue) Append(msg Message){
	q.Messages = append(q.Messages, msg)
}

func (q *Queue) MatchFilters(key RoutingKey) bool{
	for _,v := range q.Filters{
		if v.Match(key){
			return true
		}
	}
	return false
}


