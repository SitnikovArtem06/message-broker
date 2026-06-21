package brokerclient

import (
	brokerpb "github.com/SitnikovArtem06/message-broker/proto"
)

type MessageStream struct {
	client       *Client
	exchangeName string
	queueName    string
	stream       brokerpb.BrokerService_StreamFetchClient
}

func (s *MessageStream) Receive() (*Message, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		return nil, err
	}

	return s.client.newMessage(s.exchangeName, s.queueName, resp.GetDelivery()), nil
}
