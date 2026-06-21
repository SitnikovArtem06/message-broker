package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/SitnikovArtem06/message-broker/internal/config"
	"github.com/SitnikovArtem06/message-broker/internal/core"
	"github.com/SitnikovArtem06/message-broker/internal/db"
	"github.com/SitnikovArtem06/message-broker/internal/repository"
	"github.com/SitnikovArtem06/message-broker/internal/service"
	brokergrpc "github.com/SitnikovArtem06/message-broker/internal/transport/grpc"
	brokerpb "github.com/SitnikovArtem06/message-broker/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	appConfig, err := config.Load()
	if err != nil {
		logger.Error("failed to load app config", "err", err)
		os.Exit(1)
	}

	pool, err := db.Connect(ctx, appConfig.DB)
	if err != nil {
		logger.Error("failed to connect db", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	repo := repository.NewRepository(pool)
	broker := core.NewBroker()
	brokerService := service.NewBrokerService(broker, repo, appConfig.Limits)

	if err := brokerService.Restore(ctx); err != nil {
		logger.Error("failed to restore broker state", "err", err)
		os.Exit(1)
	}

	listener, err := net.Listen("tcp", appConfig.GRPCAddr)
	if err != nil {
		logger.Error("failed to listen", "addr", appConfig.GRPCAddr, "err", err)
		os.Exit(1)
	}

	authInterceptor := brokergrpc.NewAuthInterceptor(appConfig.Auth.RootToken)
	server := grpc.NewServer(
		grpc.UnaryInterceptor(authInterceptor.Unary()),
		grpc.StreamInterceptor(authInterceptor.Stream()),
	)
	handler := brokergrpc.NewBrokerHandler(brokerService)
	brokerpb.RegisterBrokerServiceServer(server, handler)
	reflection.Register(server)

	go func() {
		<-ctx.Done()
		logger.Info("service shutdowned")
		server.GracefulStop()
	}()

	logger.Info("service started", "addr", appConfig.GRPCAddr)
	if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		logger.Error("grpc server stopped with error", "err", err)
		os.Exit(1)
	}
	logger.Info("service stopped")
}
