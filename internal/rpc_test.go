package internal_test

import (
	"net"
	"net/rpc"
	"testing"

	"github.com/go-jose/go-jose/v4/testutils/assert"
	"github.com/nolannguyen1212/go-playground/internal"
)

func newClient(t *testing.T) *rpc.Client {
	t.Helper()

	server := rpc.NewServer()
	if err := server.RegisterName("RPCService", new(internal.RPCService)); err != nil {
		t.Fatal(err)
	}

	serverConn, clientConn := net.Pipe()
	go server.ServeConn(serverConn)

	client := rpc.NewClient(clientConn)
	t.Cleanup(func() { client.Close() })

	return client
}

func TestRequest(t *testing.T) {
	client := newClient(t)

	var reply string
	err := client.Call("RPCService.Request", "ping", &reply)

	assert.Equal(t, err, nil)
	assert.Equal(t, reply, "rpc requested: ping")
}
