package core

import (
	errs "github.com/SitnikovArtem06/message-broker/internal/errors"
)

type Exchange struct {
	Name   string
	Queues map[string]*Queue
}


func (e *Exchange) RegisterQueue(name string,IsDurable bool, IsAutoDelete bool, filters []RoutingFilter) (*Queue, error){
	if queue,ok := e.Queues[name]; ok{
		if queue.IsDurable == IsDurable && queue.IsAutoDelete == IsAutoDelete && equalFilters(queue.Filters, filters){
			return queue, nil
		}
		return nil, errs.QueueAlreadyExist
	}

	if IsDurable && IsAutoDelete{
		return nil, errs.QueueFlagsConflict
	}

	if !validFilters(filters){
		return nil, errs.FiltersIncorrect
	}

	queue := &Queue{Name: name, IsDurable: IsDurable, IsAutoDelete: IsAutoDelete, Filters: filters}

	e.Queues[name] = queue

	return queue, nil
}

func (e *Exchange) DeleteQueue(name string) error{
	if _,ok := e.Queues[name];!ok{
		return errs.QueueNotFound
	}

	delete(e.Queues, name)
	return nil
}

func validFilters(filters []RoutingFilter) bool{
	for _,v := range filters{
		if !v.IsValid(){
			return false
		}
	}
	return true
}

func (e *Exchange) Publish(routingKey string, payload []byte) error{
	key := RoutingKey(routingKey)
	if !key.IsValid(){
		return errs.InvalidRoutingKey
	}

	msg := Message{RoutingKey: key, Payload: payload}
	for _,q := range e.Queues{
		if q.MatchFilters(key){
			q.Append(msg)
		}
	}

	return nil
}

func (e *Exchange) Fetch(queueName string) ([]byte, error){
	queue,ok := e.Queues[queueName]; 
	if !ok{
		return nil, errs.QueueNotFound
	}
	
	if msg, err := queue.Fetch(); err != nil{
		return nil,err
	}else{
		return msg.Payload, nil
	}

}

func equalFilters(a,b []RoutingFilter) bool{
	if len(a)!=len(b){
		return false
	}
	mapA := make(map[RoutingFilter]struct{})
	mapB := make(map[RoutingFilter]struct{})

	for i:=0;i<len(a);i++{
		mapA[a[i]]=struct{}{}
		mapB[b[i]]=struct{}{}
	}

	for v := range mapA{
		if _,ok := mapB[v];!ok{
			return false
		}
	}

	return true
}

