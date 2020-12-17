package grpc

import (
	"github.com/influxdata/influxdb/services/replication"
	"google.golang.org/grpc"
	"net"
)

type Service struct {
	Listener   net.Listener
	Store      store
	TSDBStore  replication.TSDBStore
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
