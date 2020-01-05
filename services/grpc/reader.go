package grpc

import "github.com/influxdata/flux"

type Reader struct {
	Addr [][] string
}

func (r *Reader) Read(spec flux.Spec) (flux.TableIterator, error) {
	tables := &tables{m: map[string]*mergeTable{}}
	for _, addr := range r.Addr {
		client := Client{
			Addrs: addr,
		}
		colReaders, err := client.Read(spec)
		if err != nil {
			return nil, err
		}
		for colReader := range colReaders {
			tables.Add(colReader)
		}
	}
	return tables, nil
}
