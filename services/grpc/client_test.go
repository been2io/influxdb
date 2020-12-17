package grpc

import (
	"context"
	"github.com/influxdata/influxdb/services/replication"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"google.golang.org/grpc"
	"testing"
	"time"
)

func Test_replication(t *testing.T) {
	conn, err := grpc.Dial("localhost:7001",grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	c := grpcClient{
		client: datatypes.NewStorageClient(conn),
	}
	err=c.ImportPartition(context.Background(), replication.PartitionImportRequest{
		Addr:      "localhost:7000",
		DB:        "influxdb-monitor",
		Partition: "default-1603871136-0",
		Start: time.Now().Add(-10 *time.Hour),
		End: time.Now(),
	})
	if err!=nil{
		panic(err)
	}
}
