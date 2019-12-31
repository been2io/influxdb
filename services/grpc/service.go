package grpc

import (
	"net"
)

type Service struct {
	Listener   net.Listener
	Store      store
	controller controller
}

func (s *Service) Open() error {
	server := server{
		controller: s.controller,
		store:      s.Store,
	}
	go server.Serve(s.Listener)
	return nil
}
