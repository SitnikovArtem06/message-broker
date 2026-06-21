package broker_handler

import (
	"context"
	"log/slog"

	errs "github.com/SitnikovArtem06/message-broker/internal/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const rootTokenHeader = "x-root-token"

type AuthInterceptor struct {
	rootToken string
}

func NewAuthInterceptor(rootToken string) *AuthInterceptor {
	return &AuthInterceptor{rootToken: rootToken}
}

func (i *AuthInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if err := i.authorize(ctx); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

func (i *AuthInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if err := i.authorize(ss.Context()); err != nil {
			return err
		}

		return handler(srv, ss)
	}
}

func (i *AuthInterceptor) authorize(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		slog.Warn("authentication failed", "reason", "missing_metadata")
		return status.Error(codes.Unauthenticated, errs.Unauthorized.Error())
	}

	values := md.Get(rootTokenHeader)
	if len(values) == 0 || values[0] != i.rootToken {
		slog.Warn("authentication failed", "reason", "invalid_root_token")
		return status.Error(codes.Unauthenticated, errs.Unauthorized.Error())
	}

	return nil
}
