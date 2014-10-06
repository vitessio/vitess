// +build linux

// Only build on Linux, since the use of procfs is platform specific.

// Package executil provides information about running processes.
package executil

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"syscall"
)

// Incomplete interpretation of the /proc/pid/stat file.
type procStat struct {
	pid       int
	cmd       string
	state     string
	ppid      int
	pgrp      int
	sessionId int
}

func readProcStats(pid int) (*procStat, error) {
	fname := fmt.Sprintf("/proc/%v/stat", pid)
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		return nil, err
	}

	i := bytes.Index(data, []byte(" ("))
	j := bytes.Index(data, []byte(") "))
	stats := procStat{}
	stats.pid, err = strconv.Atoi(string(data[:i]))
	if err != nil {
		return nil, fmt.Errorf("invalid pid in %v %v", fname, err)
	}
	stats.cmd = string(data[i+2 : j])
	fields := string(data[j+2:])
	_, err = fmt.Sscanf(fields, "%s %d %d %d", &stats.state, &stats.ppid, &stats.pgrp, &stats.sessionId)
	if err != nil {
		return nil, fmt.Errorf("invalid scan in %v %v \"%v\"", fname, err, fields)
	}
	return &stats, nil
}

// Is there no better way to scan child / group processes?
// For now, we have to load everything we can read see if it
// matches.
func readAllProcStats() ([]*procStat, error) {
	dirEntries, err := ioutil.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	groupStats := make([]*procStat, 0, len(dirEntries))
	for _, ent := range dirEntries {
		if pid, err := strconv.Atoi(ent.Name()); err == nil {
			pidStats, err := readProcStats(pid)
			if err != nil {
				if os.IsNotExist(err) {
					// NOTE(msolo) There are inherent races here. If a process
					// disappears betweenthe time you read the directory and you
					// manage to read the proc stats, don't panic. Soldier on.
					continue
				}
				return nil, err
			}
			groupStats = append(groupStats, pidStats)
		}
	}
	return groupStats, nil
}

// GetPgrpPids return a list of all pids in a given process group.
// Not as cheap as you think, you have to scan all the pids on the system.
func GetPgrpPids(pgrp int) ([]int, error) {
	stats, err := readAllProcStats()
	if err != nil {
		return nil, err
	}
	pids := make([]int, 0, 32)
	for _, st := range stats {
		if st.pgrp == pgrp {
			pids = append(pids, st.pid)
		}
	}
	if len(pids) == 0 {
		return nil, syscall.ESRCH
	}
	return pids, nil
}

// GetPgrpPids return a list of all pids in a given process group.
// Not as cheap as you think, you have to scan all the pids on the system.
func GetSessionIdPids(sessionId int) ([]int, error) {
	stats, err := readAllProcStats()
	if err != nil {
		return nil, err
	}
	pids := make([]int, 0, 32)
	for _, st := range stats {
		if st.sessionId == sessionId {
			pids = append(pids, st.pid)
		}
	}
	if len(pids) == 0 {
		return nil, syscall.ESRCH
	}
	return pids, nil
}
