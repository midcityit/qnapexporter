package prometheus

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pedropombeiro/qnapexporter/lib/utils"
)

// volConcurrency bounds how many getsysinfo calls run at once during volume
// discovery. On QuTS hero, sysvolnum over-reports (counts empty pool slots),
// so the index space can be large (~60) while real volumes are few. Fanning
// the per-index calls out with a small pool turns a multi-second serial walk
// into roughly one call's latency, without stampeding getsysinfo.
const volConcurrency = 8

type volumeInfo struct {
	index                         string
	fileSystem                    string
	description                   string
	status                        string
	freeSizeBytes, totalSizeBytes float64
}

func (e *promExporter) readSysVolInfo() {
	volCount := 0
	sysvolnumOutput, err := utils.ExecCommand(e.getsysinfo, "sysvolnum")
	if err == nil {
		volCount, err = strconv.Atoi(sysvolnumOutput)
		if err != nil {
			volCount = 0
		}
	}
	e.Logger.Printf("Retrieved volCount: %d", volCount)

	if volCount <= 0 {
		e.volumes = nil
		return
	}

	// sysvolnum over-reports on QuTS hero: it counts empty pool slots, so the
	// real volumes are sparse within the index space. Probe a generous index
	// range in parallel and keep only indices whose description parses to a
	// real volume. Scan up to 4x volCount (capped) so sparsely-numbered real
	// volumes beyond the nominal count are still found.
	scanLimit := volCount * 4
	if scanLimit > 256 {
		scanLimit = 256
	}

	// results[i] is nil until a goroutine fills it; indexed writes keep this
	// lock-free (each goroutine owns a distinct index).
	results := make([]*volumeInfo, scanLimit)

	sem := make(chan struct{}, volConcurrency)
	var wg sync.WaitGroup
	for i := 0; i < scanLimit; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			results[idx] = e.probeVolume(uint64(idx))
		}(i)
	}
	wg.Wait()

	// Collect in index order for deterministic output; stop once we've gathered
	// volCount real volumes.
	e.volumes = make([]volumeInfo, 0, volCount)
	for _, v := range results {
		if v == nil {
			continue
		}
		e.volumes = append(e.volumes, *v)
		if len(e.volumes) >= volCount {
			break
		}
	}

	e.Logger.Printf("Found volumes %v", e.volumes)
}

// probeVolume fetches the descriptor fields for a single volume index. It
// returns nil for empty/non-volume slots or on any error, so callers can
// simply skip nil entries. All getsysinfo calls are individually timeout-bound
// via utils.ExecCommand.
func (e *promExporter) probeVolume(idx uint64) *volumeInfo {
	volIdx := strconv.FormatUint(idx, 10)

	desc, err := utils.ExecCommand(e.getsysinfo, "vol_desc", volIdx)
	if err != nil {
		return nil
	}
	description := parseVolDesc(desc)
	if description == "" {
		return nil
	}

	// Honor the optional exclude filter before any further getsysinfo calls,
	// so filtered volumes cost nothing and produce no series.
	if e.volumeExcludeRe != nil && e.volumeExcludeRe.MatchString(description) {
		return nil
	}

	fileSystem, err := utils.ExecCommand(e.getsysinfo, "vol_fs", volIdx)
	if err != nil {
		e.Logger.Printf("Error fetching volume %q file system: %v", description, err)
		return nil
	}
	// QuTS hero (ZFS-based) reports "Unknown" for all volumes since ZFS datasets
	// don't map to traditional filesystem names in getsysinfo. Treat as "zfs".
	if fileSystem == "Unknown" {
		fileSystem = "zfs"
	}

	volsizeStr, err := utils.ExecCommand(e.getsysinfo, "vol_totalsize", volIdx)
	if err != nil {
		e.Logger.Printf("Error fetching volume %q size: %v", description, err)
		return nil
	}
	volsizeBytes, err := parseVolSize(volsizeStr)
	if err != nil {
		return nil
	}

	status, err := utils.ExecCommand(e.getsysinfo, "vol_status", volIdx)
	if err != nil {
		e.Logger.Printf("Error fetching volume %q status: %v", description, err)
		return nil
	}

	return &volumeInfo{
		index:          volIdx,
		description:    description,
		fileSystem:     fileSystem,
		status:         status,
		totalSizeBytes: volsizeBytes,
	}
}

func (e *promExporter) getSysInfoVolMetrics() ([]metric, error) {
	if e.getsysinfo == "" {
		return nil, nil
	}

	metrics := make([]metric, 0, 2*len(e.volumes))
	e.status.Volumes = []string{}

	expired := e.volumeLastFetch.IsZero() || time.Now().After(e.volumeLastFetch.Add(volumeValidity))
	if expired {
		e.volumeLastFetch = time.Now()

		// Refresh free size for all volumes in parallel; each goroutine owns a
		// distinct slice index so the writes are lock-free.
		sem := make(chan struct{}, volConcurrency)
		var wg sync.WaitGroup
		for idx := range e.volumes {
			if e.volumes[idx].freeSizeBytes != 0 {
				continue
			}
			wg.Add(1)
			sem <- struct{}{}
			go func(i int) {
				defer wg.Done()
				defer func() { <-sem }()
				freesizeStr, err := utils.ExecCommand(e.getsysinfo, "vol_freesize", e.volumes[i].index)
				if err != nil {
					e.Logger.Printf("Error fetching volume %q free size: %v", e.volumes[i].description, err)
					return
				}
				freeSizeBytes, err := parseVolSize(freesizeStr)
				if err != nil {
					return
				}
				e.volumes[i].freeSizeBytes = freeSizeBytes
			}(idx)
		}
		wg.Wait()
	}

	for _, v := range e.volumes {
		e.status.Volumes = append(e.status.Volumes, v.description)

		attr := fmt.Sprintf("volume=%q,filesystem=%q,status=%q", v.description, v.fileSystem, v.status)
		newMetrics := []metric{
			{
				name:  "node_volume_avail_bytes",
				attr:  attr,
				value: v.freeSizeBytes,
			},
			{
				name:  "node_volume_size_bytes",
				attr:  attr,
				value: v.totalSizeBytes,
			},
		}
		metrics = append(metrics, newMetrics...)
	}

	return metrics, nil
}

func parseVolDesc(desc string) string {
	var index int
	switch {
	case strings.HasPrefix(desc, "[Volume"):
		index = 8
	case strings.HasPrefix(desc, "[Single Disk Volume:"):
		return ""
	default:
		return desc
	}

	name := strings.SplitN(strings.TrimSpace(desc[index:]), ",", 2)[0]
	// Trim trailing bracket for cases like "[Volume MyVol]" (no pool info)
	return strings.TrimSuffix(strings.TrimSpace(name), "]")
}

func parseVolSize(s string) (float64, error) {
	fields := strings.Fields(s)
	size, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, fmt.Errorf("parse volume size (%s): %w", s, err)
	}

	var factor float64
	switch fields[1] {
	case "TB":
		factor = 1024 * 1024 * 1024 * 1024
	case "GB":
		factor = 1024 * 1024 * 1024
	case "MB":
		factor = 1024 * 1024
	case "KB":
		factor = 1024
	case "B":
		factor = 1
	}

	return size * factor, nil
}
