package grpc

import (
	"google.golang.org/grpc"
	"net"
)

type Service struct {
	Listener   net.Listener
	Store      store
	Controller controller
}

func (s *Service) Open() *grpc.Server {
	server := server{
		controller: s.Controller,
		store:      s.Store,
	}
	return server.Serve(s.Listener)
}
