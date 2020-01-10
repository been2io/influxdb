package grpc

import (
	"context"
	"encoding/json"
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
	"time"
)

type Client struct {
	Addrs []string
}

func (c *Client) Read(spec flux.Spec) (chan flux.ColReader, error) {
	address := c.Addrs[0]
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	storageClient := datatypes.NewStorageClient(conn)
	request := ProxyRequest{
		Spec: spec,
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)
	client, err := storageClient.ExecSpec(ctx, &datatypes.SpecRequest{Request: body})
	if err != nil {
		return nil, err
	}
	ch := make(chan flux.ColReader)
	go func() {
		defer conn.Close()
		defer close(ch)
		for {
			resp, err := client.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				panic(err)
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
			var values [] array.Interface
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
					}
					values = append(values, builder.NewArray())

				}
				return cols
			}()
			tb := &arrow.TableBuffer{
				GroupKey: groupKey,
				Columns:  colMeta,
				Values:   values,
			}
			ch <- tb
		}

	}()
	return ch, nil
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
