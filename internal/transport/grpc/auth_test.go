package broker_handler

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAuthorizeRejectsMissingToken(t *testing.T) {
	interceptor := NewAuthInterceptor("root-token")

	err := interceptor.authorize(context.Background())
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("authorize() code = %v, want %v", status.Code(err), codes.Unauthenticated)
	}
}

func TestAuthorizeRejectsInvalidToken(t *testing.T) {
	interceptor := NewAuthInterceptor("root-token")
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(rootTokenHeader, "wrong-token"))

	err := interceptor.authorize(ctx)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("authorize() code = %v, want %v", status.Code(err), codes.Unauthenticated)
	}
}

func TestAuthorizeAllowsValidToken(t *testing.T) {
	interceptor := NewAuthInterceptor("root-token")
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(rootTokenHeader, "root-token"))

	if err := interceptor.authorize(ctx); err != nil {
		t.Fatalf("authorize() error = %v", err)
	}
}
