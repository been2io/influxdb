package universe

import (
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/plan"
)

func init() {
	execute.RegisterTransformation(plan.ConcurrentTransformerKind, createConcurrentTransformation)
}

type ConcurrentDataset struct {
	id  execute.DatasetID
	ts  execute.TransformationSet
	idx int
	cap int
}

func NewConcurrentDataset(id execute.DatasetID, capacity int) *ConcurrentDataset {
	return &ConcurrentDataset{id: id, cap: capacity}
}

func (d *ConcurrentDataset) AddTransformation(t execute.Transformation) {
	d.ts = append(d.ts, t)
}
func (d *ConcurrentDataset) Cap() int {
	return d.cap
}
func (d *ConcurrentDataset) Size() int {
	return len(d.ts)
}
func (d *ConcurrentDataset) Process(tbl flux.Table) error {
	i := d.idx % len(d.ts)
	d.idx++
	return d.ts[i].Process(d.id, tbl)
}

func (d *ConcurrentDataset) RetractTable(key flux.GroupKey) error {
	return d.ts.RetractTable(d.id, key)
}

func (d *ConcurrentDataset) UpdateProcessingTime(t execute.Time) error {
	return d.ts.UpdateProcessingTime(d.id, t)
}

func (d *ConcurrentDataset) UpdateWatermark(mark execute.Time) error {
	return d.ts.UpdateWatermark(d.id, mark)
}

func (d *ConcurrentDataset) Finish(err error) {
	d.ts.Finish(d.id, err)
}

func (d *ConcurrentDataset) SetTriggerSpec(t plan.TriggerSpec) {
}

type concurrentTransformer struct {
	ds *ConcurrentDataset
}

func (c *concurrentTransformer) RetractTable(id execute.DatasetID, key flux.GroupKey) error {
	panic("implement me")
}

func (c *concurrentTransformer) Process(id execute.DatasetID, tbl flux.Table) error {
	return c.ds.Process(tbl)
}

func (c *concurrentTransformer) UpdateWatermark(id execute.DatasetID, t execute.Time) error {
	return c.ds.UpdateWatermark(t)
}

func (c *concurrentTransformer) UpdateProcessingTime(id execute.DatasetID, t execute.Time) error {
	panic("implement me")
}

func (c *concurrentTransformer) Finish(id execute.DatasetID, err error) {
	c.ds.Finish(err)
}
func createConcurrentTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	ds := NewConcurrentDataset(id, 0)
	t := &concurrentTransformer{
		ds: ds,
	}
	return t, ds, nil
}
