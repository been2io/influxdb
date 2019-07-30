package monitor

import (
	"os"
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
)

var startTime time.Time

func init() {
	startTime = time.Now().UTC()
}

// system captures system-level diagnostics.
type system struct{}

func (s *system) Diagnostics() (*diagnostics.Diagnostics, error) {
	currentTime := time.Now().UTC()
	d := map[string]interface{}{
		"PID":         os.Getpid(),
		"currentTime": currentTime,
		"started":     startTime,
		"uptime":      currentTime.Sub(startTime).String(),
		"up":          currentTime.Sub(startTime),
	}

	return diagnostics.RowFromMap(d), nil
}
