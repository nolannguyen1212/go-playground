package internal_test

import (
	"context"
	"net"
	"testing"

	"github.com/nolannguyen1212/go-playground/internal"
	pb "github.com/nolannguyen1212/go-playground/proto/pb/pubsub"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func newBufconnServer(t *testing.T, broker *internal.Broker) *bufconn.Listener {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)

	grpcServer := grpc.NewServer()
	pb.RegisterPubsubServiceServer(
		grpcServer,
		internal.NewPubsubServer(broker),
	)

	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)

	return lis
}

func newBufconnClient(t *testing.T, lis *bufconn.Listener) pb.PubsubServiceClient {
	t.Helper()

	dial := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dial),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { conn.Close() })

	return pb.NewPubsubServiceClient(conn)
}

func newGrpcClient(t *testing.T) pb.PubsubServiceClient {
	t.Helper()

	broker := internal.NewBroker()
	t.Cleanup(broker.Close)

	lis := newBufconnServer(t, broker)
	return newBufconnClient(t, lis)
}

func TestPublishSubscribe(t *testing.T) {
	client := newGrpcClient(t)

	stream, err := client.Subscribe(
		context.Background(),
		&pb.SubscribeRequest{Topic: "go"},
	)
	if err != nil {
		t.Fatal(err)
	}

	go client.Publish(
		context.Background(),
		&pb.PublishRequest{
			Topic:   "go",
			Message: "hello world",
		},
	)

	msg, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if msg.GetText() != "hello world" {
		t.Fatalf("got %q, want %q", msg.GetText(), "hello world")
	}
}
