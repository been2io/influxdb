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

func (s *Service) Open() {
	server := server{
		controller: s.Controller,
		store:      s.Store,
	}
	server.Serve(s.Listener)
}
func (s *Service) Server() *grpc.Server {
	server := server{
		controller: s.Controller,
		store:      s.Store,
	}
	return server.Server()
}
