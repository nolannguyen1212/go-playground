package internal

type RPCService struct{}

func (r *RPCService) Request(request string, reply *string) error {
	*reply = "rpc requested: " + request
	return nil
}
