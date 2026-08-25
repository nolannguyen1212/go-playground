package internal

import (
	"context"

	pb "github.com/nolannguyen1212/go-playground/proto/pb/pubsub"
)

type PubsubServer struct {
	pb.UnimplementedPubsubServiceServer

	broker *Broker
}

func NewPubsubServer(broker *Broker) *PubsubServer {
	return &PubsubServer{broker: broker}
}

func (s *PubsubServer) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	s.broker.Publish(Topic(req.Topic), req.GetMessage())
	return &pb.PublishResponse{Success: true}, nil
}

func (s *PubsubServer) Subscribe(req *pb.SubscribeRequest, stream pb.PubsubService_SubscribeServer) error {
	sub := s.broker.Subscribe(Topic(req.GetTopic()))
	defer s.broker.Unsubscribe(sub)

	for {
		select {
		case msg, ok := <-sub:
			if !ok {
				return nil
			}
			if err := stream.Send(&pb.Message{Text: msg.(string)}); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}
