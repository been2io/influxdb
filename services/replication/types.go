package replication

import "time"

type ShardInfo struct {
	ID    uint64
	Start time.Time
	End   time.Time
}

type ShardRequest struct {
	DB        string
	Partition string
	Start     time.Time
	End       time.Time
}

type ShardExportRequest struct {
	ID    uint64
	Start time.Time
	End   time.Time
}

type ShardImportRequest struct {
	ID        uint64
	Start     time.Time
	End       time.Time
	Addr      string
	DB        string
	Partition string
}

type PartitionImportRequest struct {
	Start     time.Time
	End       time.Time
	Addr      string
	DB        string
	Partition string
}
