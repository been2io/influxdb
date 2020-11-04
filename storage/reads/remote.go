package reads

import (
	"context"
	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

type RemoteStore struct {
	client datatypes.StorageClient
	source *types.Any
}

func NewRemoteStore(client datatypes.StorageClient, source *types.Any) *RemoteStore {
	return &RemoteStore{
		client: client,
		source: source,
	}
}
func (r *RemoteStore) ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (ResultSet, error) {
	clone := *req
	clone.ReadSource = r.source
	resp, err := r.client.ReadFilter(ctx, &clone)
	if err != nil {
		return nil, err
	}
	return NewResultSetStreamReader(resp), nil
}

func (r *RemoteStore) ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (GroupResultSet, error) {
	clone := *req
	clone.ReadSource = r.source
	resp, err := r.client.ReadGroup(ctx, &clone)
	if err != nil {
		return nil, err
	}
	return NewGroupResultSetStreamReader(resp), nil
}

func (r *RemoteStore) TagKeys(ctx context.Context, req *datatypes.TagKeysRequest) (cursors.StringIterator, error) {
	panic("implement me")
}

func (r *RemoteStore) TagValues(ctx context.Context, req *datatypes.TagValuesRequest) (cursors.StringIterator, error) {
	panic("implement me")
}

func (r *RemoteStore) GetSource(db, rp string) proto.Message {
	return &ReadSource{
		Database: db,
		RetentionPolicy: rp,
	}
}

type MergedRemoteStore struct {
	clients []RemoteStore
}

func (r *MergedRemoteStore) ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (ResultSet, error) {
	var rrs []ResultSet
	for _, c := range r.clients {
		r, err := c.ReadFilter(ctx, req)
		if err != nil {
			return nil, err
		}
		rrs = append(rrs, r)
	}
	return NewMergedResultSet(rrs), nil
}

func (r *MergedRemoteStore) ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (GroupResultSet, error) {
	var rrs []GroupResultSet
	for _, c := range r.clients {
		r, err := c.ReadGroup(ctx, req)
		if err != nil {
			return nil, err
		}
		rrs = append(rrs, r)
	}
	switch req.Group {
	case datatypes.GroupNone:
		return NewGroupNoneMergedGroupResultSet(rrs), nil
	case datatypes.GroupBy:
		return NewGroupByMergedGroupResultSet(rrs), nil
	default:
		return nil, errors.New("MergedRemoteStore: unexpected group")
	}
}

func (r *MergedRemoteStore) TagKeys(ctx context.Context, req *datatypes.TagKeysRequest) (cursors.StringIterator, error) {
	panic("implement me")
}

func (r *MergedRemoteStore) TagValues(ctx context.Context, req *datatypes.TagValuesRequest) (cursors.StringIterator, error) {
	panic("implement me")
}
type ReadSource struct {
	// Database identifies which database to query.
	Database string `protobuf:"bytes,1,opt,name=database,proto3" json:"database,omitempty"`
	// RetentionPolicy identifies which retention policy to query.
	RetentionPolicy string `protobuf:"bytes,2,opt,name=retention_policy,json=retentionPolicy,proto3" json:"retention_policy,omitempty"`
}

func (m *ReadSource) Reset()                    { *m = ReadSource{} }
func (m *ReadSource) String() string            { return proto.CompactTextString(m) }
func (*ReadSource) ProtoMessage()               {}
func (r *MergedRemoteStore) GetSource(db, rp string) proto.Message {
	return &ReadSource{
		Database: db,
		RetentionPolicy: rp,
	}
}

type RemoteReader struct {
	clients []RemoteStore
}

func NewRemoteReader(stores []RemoteStore) influxdb.Reader {
	return &RemoteReader{
		clients: stores,
	}
}
func (r *RemoteReader) ReadFilter(ctx context.Context, spec influxdb.ReadFilterSpec, alloc *memory.Allocator) (influxdb.TableIterator, error) {
	store := &MergedRemoteStore{
		clients: r.clients,
	}
	fi := filterIterator{
		ctx:   ctx,
		s:     store,
		spec:  spec,
		alloc: alloc,
	}
	return &fi, nil

}

func (r *RemoteReader) ReadGroup(ctx context.Context, spec influxdb.ReadGroupSpec, alloc *memory.Allocator) (influxdb.TableIterator, error) {
	store := &MergedRemoteStore{
		clients: r.clients,
	}
	gi := groupIterator{
		ctx:   ctx,
		s:     store,
		spec:  spec,
		alloc: alloc,
	}
	return &gi, nil
}

func (r *RemoteReader) ReadTagKeys(ctx context.Context, spec influxdb.ReadTagKeysSpec, alloc *memory.Allocator) (influxdb.TableIterator, error) {
	panic("implement me")
}

func (r *RemoteReader) ReadTagValues(ctx context.Context, spec influxdb.ReadTagValuesSpec, alloc *memory.Allocator) (influxdb.TableIterator, error) {
	panic("implement me")
}

func (r *RemoteReader) Close() {
	panic("implement me")
}