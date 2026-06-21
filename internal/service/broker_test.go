package service

import (
	"context"
	"errors"
	"testing"

	"github.com/SitnikovArtem06/message-broker/internal/config"
	"github.com/SitnikovArtem06/message-broker/internal/core"
	errs "github.com/SitnikovArtem06/message-broker/internal/errors"
	"github.com/golang/mock/gomock"
)

func TestDeleteExchangeFailsWhenAnyQueueHasActiveConsumers(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	broker := core.NewBroker()
	repo := NewMockRepository(ctrl)
	service := NewBrokerService(broker, repo, config.LimitsConfig{
		MaxQueueFilters: 32,
	})

	repo.EXPECT().SaveExchange(ctx, "corp").Return(nil)

	if _, err := service.CreateExchange(ctx, "corp"); err != nil {
		t.Fatalf("CreateExchange() error = %v", err)
	}
	if _, err := service.RegisterQueue(ctx, "corp", "users", false, false, 0, []core.RoutingFilter{"corp.users.*"}); err != nil {
		t.Fatalf("RegisterQueue() error = %v", err)
	}
	if err := service.AddConsumer(ctx, "corp", "users", "consumer-1"); err != nil {
		t.Fatalf("AddConsumer() error = %v", err)
	}

	err := service.DeleteExchange(ctx, "corp")
	if !errors.Is(err, errs.ExchangeHasConsumers) {
		t.Fatalf("DeleteExchange() error = %v, want %v", err, errs.ExchangeHasConsumers)
	}
}
