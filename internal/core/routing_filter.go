package core

import (
	"strings"
	"regexp"
)

type RoutingFilter string

var regexFilter = regexp.MustCompile(`^([A-Za-z0-9_-]+|\*)(\.([A-Za-z0-9_-]+|\*))*$`)

func (r RoutingFilter) IsValid() bool{
	return regexFilter.Match([]byte(r))
}

func (r RoutingFilter) Match(key RoutingKey) bool {

	splitFilter := strings.Split(string(r),".")
	splitKey := strings.Split(string(key),".")

	if len(splitFilter) != len(splitKey){
		return false
	}

	for i,v := range splitFilter{
		if v == "*"{
			continue
		}
		if v != splitKey[i]{
			return false
		}
	}
	return true

}