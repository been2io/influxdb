package plan

import (
	"github.com/influxdata/flux"
)

func IsPushDownOp(o *flux.Operation) bool {
	switch o.Spec.Kind() {
	case "range":
	case "filter":
	case "window":
	case "first":
	case "last":
	case "sum":
	case "sample":
	case "group":
	case "from":
	case "influxDBFrom":
	default:
		return false
	}
	return true
}
