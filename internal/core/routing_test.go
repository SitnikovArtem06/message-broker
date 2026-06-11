package core

import "testing"

func TestRoutingKeyIsValid(t *testing.T) {
	tests := []struct {
		name  string
		key   RoutingKey
		valid bool
	}{
		{name: "valid key", key: "corp.data-base.users.create", valid: true},
		{name: "empty token", key: "corp..users", valid: false},
		{name: "leading dot", key: ".corp.users", valid: false},
		{name: "trailing dot", key: "corp.users.", valid: false},
		{name: "wildcard is invalid for key", key: "corp.users.*", valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.key.IsValid(); got != tt.valid {
				t.Fatalf("IsValid() = %v, want %v", got, tt.valid)
			}
		})
	}
}

func TestRoutingFilterIsValid(t *testing.T) {
	tests := []struct {
		name   string
		filter RoutingFilter
		valid  bool
	}{
		{name: "exact filter", filter: "corp.users.create", valid: true},
		{name: "wildcard filter", filter: "corp.users.*", valid: true},
		{name: "single wildcard", filter: "*", valid: true},
		{name: "invalid embedded wildcard", filter: "corp.us*ers.create", valid: false},
		{name: "empty token", filter: "corp..create", valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.filter.IsValid(); got != tt.valid {
				t.Fatalf("IsValid() = %v, want %v", got, tt.valid)
			}
		})
	}
}

func TestRoutingFilterMatch(t *testing.T) {
	tests := []struct {
		name   string
		filter RoutingFilter
		key    RoutingKey
		match  bool
	}{
		{
			name:   "exact match",
			filter: "corp.data-base.users.create",
			key:    "corp.data-base.users.create",
			match:  true,
		},
		{
			name:   "wildcard match",
			filter: "corp.data-base.users.*",
			key:    "corp.data-base.users.delete",
			match:  true,
		},
		{
			name:   "different token count",
			filter: "corp.data-base.users.*",
			key:    "corp.data-base.users.create.extra",
			match:  false,
		},
		{
			name:   "different token value",
			filter: "corp.data-base.*.create",
			key:    "corp.data-base.groups.delete",
			match:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.filter.Match(tt.key); got != tt.match {
				t.Fatalf("Match() = %v, want %v", got, tt.match)
			}
		})
	}
}
