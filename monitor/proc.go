package monitor

import (
	"expvar"
	"fmt"
	"github.com/influxdata/influxdb/models"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/process"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

var p *process.Process

func init() {
	pid := os.Getpid()
	if p1, err := process.NewProcess(int32(pid)); err == nil {
		p = p1
	}

}

func SystemStats(datadir string, tags map[string]string) ([]*models.Statistic) {
	var stats []*models.Statistic
	if p != nil {
		if cpu, err := p.Percent(0); err == nil {
			stat := models.NewStatistic("cpu")
			stat.Values["usedPercent"] = cpu
			stat.Values["percent"] = cpu / float64(runtime.NumCPU())
			stat.Values["num"] = runtime.NumCPU()
			stats = append(stats, &stat)

		}
		if io, err := p.IOCounters(); err == nil {
			stat := models.NewStatistic("io")
			stat.Values["readBytes"] = io.ReadBytes
			stat.Values["writeBytes"] = io.WriteBytes
			stat.Values["reads"] = io.ReadCount
			stat.Values["writes"] = io.WriteCount
			stats = append(stats, &stat)
		}
		if ios, err := p.NetIOCounters(true); err == nil {
			for _, io := range ios {
				stat := models.NewStatistic("net")
				stat.Tags["name"] = io.Name
				stat.Values["bytesRecv"] = io.BytesRecv
				stat.Values["bytesSend"] = io.BytesSent
				stat.Values["err"] = io.Errin + io.Errout
				stats = append(stats, &stat)
			}
		}
		if conns, err := p.Connections(); err == nil {
			m := expvar.Map{}
			for _, conn := range conns {
				m.Add(fmt.Sprintf("%v!%v", conn.Status, conn.Raddr.IP), 1)
			}
			m.Do(func(value expvar.KeyValue) {
				if v, ok := value.Value.(*expvar.Int); ok {
					keys := strings.Split(value.Key, "!")
					if len(keys) == 2 {
						stat := models.NewStatistic("conns")
						stat.Tags["to"] = keys[1]
						stat.Tags["status"] = keys[0]
						stat.Values["number"] = v.Value()
						stats = append(stats, &stat)
					}

				}
			})
		}
	}

	for _, path := range []string{datadir, "/"} {
		if usage, err := disk.Usage(path); err == nil {
			stat := models.Statistic{
				Name: "disk",
				Tags: map[string]string{
					"dir":  path,
					"path": usage.Path,
					"fs":   usage.Fstype,
				},
				Values: map[string]interface{}{
					"free":             usage.Free,
					"used":             usage.Used,
					"total":            usage.Total,
					"userPercent":      usage.UsedPercent,
					"inodeUsed":        usage.InodesUsed,
					"inodeTotal":       usage.InodesTotal,
					"inodeUsedPercent": usage.InodesUsedPercent,
				},
			}
			stats = append(stats, &stat)
		}
	}
	if dirs, err := ioutil.ReadDir(datadir); err == nil {
		for _, d := range dirs {
			if size, err := DirSize(fmt.Sprintf("%v/%v", datadir, d.Name())); err == nil {
				stat := &models.Statistic{
					Name: "dbDisk",
					Tags: map[string]string{
						"name": d.Name(),
					},
					Values: map[string]interface{}{
						"size": size,
					},
				}
				stats = append(stats, stat)
			}
		}
	}
	for _, stat := range stats {
		for k, v := range tags {
			stat.Tags[k] = v
		}

	}
	return stats
}
func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}
