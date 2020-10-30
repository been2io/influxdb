package query

import (
	"context"
	"github.com/influxdata/influxql"
)

func BuildAuxIterator(ctx context.Context, ic IteratorCreator, sources influxql.Sources, opt IteratorOptions) (Iterator, error) {
	return buildAuxIterator(ctx, ic, sources, opt)
}
