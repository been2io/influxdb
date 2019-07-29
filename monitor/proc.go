package monitor

import (
	"fmt"
	"github.com/influxdata/influxdb/models"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/process"
	"io/ioutil"
	"os"
	"path/filepath"
)

func SystemStats(datadir string) ([]*models.Statistic) {
	var stats []*models.Statistic
	pid := os.Getpid()
	if p, err := process.NewProcess(int32(pid)); err == nil {
		if cpu, err := p.Percent(0); err == nil {
			stat := models.NewStatistic("cpu")
			stat.Values["usedPercent"] = cpu
			stats = append(stats, &stat)

		}
		if fds, err := p.OpenFiles(); err == nil {
			for _, fd := range fds {
				stat := models.NewStatistic("fd")
				stat.Tags["path"] = fd.Path
				stat.Values["size"] = fd.Fd
				stats = append(stats, &stat)

			}
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
