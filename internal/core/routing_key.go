package core

import "regexp"

type RoutingKey string

var regexKey = regexp.MustCompile(`^[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)*$`)

func (r RoutingKey) IsValid() bool{
	return regexKey.Match([]byte(r))
}
