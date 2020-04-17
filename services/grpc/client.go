package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/arrow"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"google.golang.org/grpc"
	"io"
)

type Client struct {
	Addrs []string
}
type result struct {
	reader flux.ColReader
	err    error
}
type tableReader struct {
	client datatypes.Storage_ExecSpecClient
	f      func(r flux.ColReader) error
	closed chan struct{}
}

func (r *tableReader) handleRead(ctx context.Context) error {
	client := r.client
	resp, err := client.Recv()
	if err == io.EOF {
		close(r.closed)
		return nil
	}
	if err != nil {
		return err
	}
	groupKey := func() flux.GroupKey {
		var cols []flux.ColMeta
		var v []values.Value
		for _, keys := range resp.GroupKeys.Values {
			t, err := values.NewFromString(semantic.Nature(keys.Nature), keys.Str)
			if err != nil {
				panic(err)
			}
			v = append(v, t)

		}
		for _, c := range resp.GroupKeys.Meta {
			cols = append(cols, flux.ColMeta{
				Label: c.Label,
				Type:  flux.ColType(c.Type),
			})
		}
		return execute.NewGroupKey(cols, v)
	}()
	var values []array.Interface
	colMeta := func() []flux.ColMeta {
		var cols []flux.ColMeta
		for i, m := range resp.ColumnMeta {
			cols = append(cols, flux.ColMeta{
				Label: m.Label,
				Type:  flux.ColType(m.Type),
			})
			builder := arrow.NewBuilder(flux.ColType(m.Type), &memory.Allocator{})
			switch flux.ColType(m.Type) {
			case flux.TTime:
				for _, v := range resp.Frames[i].GetIntegerPoints().Values {
					arrow.AppendInt(builder, v)

				}
			case flux.TFloat:
				for _, v := range resp.Frames[i].GetFloatPoints().Values {
					arrow.AppendFloat(builder, v)

				}
			case flux.TInt:
				for _, v := range resp.Frames[i].GetIntegerPoints().Values {
					arrow.AppendInt(builder, v)

				}
			case flux.TString:
				for _, v := range resp.Frames[i].GetStringPoints().Values {
					arrow.AppendString(builder, v)

				}
			case flux.TBool:
				panic("reader not support bool")
			case flux.TUInt:
				for _, v := range resp.Frames[i].GetUnsignedPoints().Values {
					arrow.AppendUint(builder, v)
				}
			}
			values = append(values, builder.NewArray())

		}
		return cols
	}()
	timeIdx := execute.ColIdx("_time", colMeta)
	if timeIdx == -1 {
		startIdx := execute.ColIdx("_start", colMeta)
		if startIdx == -1 {
			return errors.New("_time and _start column required")
		}
		colMeta = append(colMeta, flux.ColMeta{Type: flux.TTime, Label: "_time"})
		values = append(values, values[startIdx])
	}
	tb := &arrow.TableBuffer{
		GroupKey: groupKey,
		Columns:  colMeta,
		Values:   values,
	}
	return r.f(tb)
}
func (r *tableReader) handleReads(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-r.closed:
			return nil
		default:
		}
		err := r.handleRead(ctx)
		if err != nil {
			return err
		}
	}
}
func (c *Client) Read(ctx context.Context, spec flux.Spec, f func(r flux.ColReader) error) error {
	address := c.Addrs[0]
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	storageClient := datatypes.NewStorageClient(conn)
	request := ProxyRequest{
		Spec: spec,
	}
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	client, err := storageClient.ExecSpec(ctx, &datatypes.SpecRequest{Request: body})
	if err != nil {
		return err
	}
	r := tableReader{
		client: client,
		f:      f,
		closed: make(chan struct{}),
	}
	return r.handleReads(ctx)

}
func AddNewTableCols(cols []flux.ColMeta, builder execute.TableBuilder, colMap []int) ([]int, error) {
	existing := builder.Cols()
	if l := len(builder.Cols()); cap(colMap) < l {
		colMap = make([]int, len(builder.Cols()))
	} else {
		colMap = colMap[:l]
	}

	for j := range colMap {
		colMap[j] = -1
	}

	for j, c := range cols {
		found := false
		for ej, ec := range existing {
			if c.Label == ec.Label {
				if c.Type == ec.Type {
					colMap[ej] = j
					found = true
					break
				} else {
					return nil, fmt.Errorf("schema collision detected: column \"%s\" is both of type %s and %s", c.Label, c.Type, ec.Type)
				}
			}
		}
		if !found {
			if _, err := builder.AddCol(c); err != nil {
				return nil, err
			}
			colMap = append(colMap, j)
		}
	}
	return colMap, nil
}
