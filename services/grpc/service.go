package grpc

import (
	"net"
)

type Service struct {
	Listener   net.Listener
	Store      store
	Controller controller
}

func (s *Service) Open()  {
	server := server{
		controller: s.Controller,
		store:      s.Store,
	}
	server.Serve(s.Listener)
}
