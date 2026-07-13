package internal

import (
	"io"
	"log"

	do "github.com/MBI-88/dominus-proto-definition/dominus"
	"github.com/MBI-88/dominus-sdk/dominus"
)

type brokerSvc struct {
	do.UnimplementedBrokerAPIServer
}

func NewBrokerSvc(reg dominus.BrokerRegister) dominus.Server {
	return reg.RegisterHandler(&brokerSvc{})
}

// StreamServerConn handles server-initiated streams.
func (s *brokerSvc) StreamServerConn(req *do.StreamRequestMessage, stream do.BrokerAPI_ServerStreamServer) error {
	count := 1000
	for count > 0 {
		// Send outbound messages to client
		if err := stream.Send(&do.StreamResponseMessage{
			Payload: []byte("Server says hello"),
			Status:  0,
		}); err != nil {
			return err
		}
		count--
	}

	return nil
}

// StreamClientConn handles client-initiated streams.
func (s *brokerSvc) StreamClientConn(stream do.BrokerAPI_ServerStreamClient) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		log.Printf("Received: %s from %v", msg.GetPayload(), msg.GetStatus())
	}
}

// BiStreamConn handles bidirectional streams.
func (s *brokerSvc) BiStreamConn(stream do.BrokerAPI_BidirectionalStreamServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		log.Printf("Bidirectional Stream received: %s", msg.Payload)
		if err := stream.Send(&do.StreamResponseMessage{
			Payload: []byte("ACK"),
		}); err != nil {
			return err
		}
	}
}
