/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package osutil

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

const (
	// portFileTimeout is how long port reservations remain valid in the port file.
	// Acts as a safety net for processes that crash without calling UnreservePorts.
	portFileTimeout = 1 * time.Hour
)

// PortReservation represents an inclusive range of reserved TCP ports.
type PortReservation struct {
	Start, End int
	allocated  time.Time
}

var (
	randomPortMu    sync.Mutex
	allocatedRanges []*PortReservation
	portFilePath    string
)

// SetPortFilePath enables file-based port tracking with locking for cross-process coordination.
func SetPortFilePath(path string) {
	randomPortMu.Lock()
	defer randomPortMu.Unlock()
	portFilePath = path
}

func hasRangeOverlap(ranges []*PortReservation, from, to int) bool {
	for _, r := range ranges {
		if from <= r.End && to >= r.Start {
			return true
		}
	}
	return false
}

func readPortFile(f *os.File) []*PortReservation {
	var ranges []*PortReservation
	now := time.Now()
	if _, err := f.Seek(0, 0); err != nil {
		panic(fmt.Sprintf("osutil: failed to seek port file: %v", err))
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var start, end int
		var ts int64
		if _, err := fmt.Sscanf(scanner.Text(), "%d %d %d", &start, &end, &ts); err != nil {
			continue
		}
		if allocated := time.Unix(ts, 0); now.Sub(allocated) <= portFileTimeout {
			ranges = append(ranges, &PortReservation{
				Start:     start,
				End:       end,
				allocated: allocated,
			})
		}
	}
	if err := scanner.Err(); err != nil {
		panic(fmt.Sprintf("osutil: failed to read port file: %v", err))
	}
	return ranges
}

func writePortFile(f *os.File, ranges []*PortReservation) {
	if err := f.Truncate(0); err != nil {
		panic(fmt.Sprintf("osutil: failed to truncate port file: %v", err))
	}
	if _, err := f.Seek(0, 0); err != nil {
		panic(fmt.Sprintf("osutil: failed to seek port file: %v", err))
	}
	for _, r := range ranges {
		if _, err := fmt.Fprintf(f, "%d %d %d\n", r.Start, r.End, r.allocated.Unix()); err != nil {
			panic(fmt.Sprintf("osutil: failed to write port file: %v", err))
		}
	}
}

// openAndLockPortFile opens and locks the port file if configured.
// Returns nil if no port file is configured; caller must call cleanup when done.
func openAndLockPortFile() (f *os.File, cleanup func()) {
	if portFilePath == "" {
		return nil, func() {}
	}
	var err error
	f, err = os.OpenFile(portFilePath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		panic(fmt.Sprintf("osutil: failed to open port file: %v", err))
	}
	lockPortFile(f)
	return f, func() {
		unlockPortFile(f)
		f.Close()
	}
}

// pruneExpiredRanges removes expired entries from allocatedRanges.
func pruneExpiredRanges() {
	now := time.Now()
	filtered := allocatedRanges[:0]
	for _, r := range allocatedRanges {
		if now.Sub(r.allocated) <= portFileTimeout {
			filtered = append(filtered, r)
		}
	}
	allocatedRanges = filtered
}

// mergeFileRanges merges port file ranges into the in-memory list.
func mergeFileRanges(f *os.File) {
	if f == nil {
		return
	}
	for _, r := range readPortFile(f) {
		if !hasRangeOverlap(allocatedRanges, r.Start, r.End) {
			allocatedRanges = append(allocatedRanges, r)
		}
	}
}

func randomPort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("osutil: failed to listen: %v", err))
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func portsAvailable(base, count int) bool {
	for i := range count {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+i))
		if err != nil {
			return false
		}
		ln.Close()
	}
	return true
}

// GetPortReservation selects count consecutive currently available TCP ports on 127.0.0.1.
// This is a best-effort, cooperative reservation mechanism: ranges are tracked in-memory by
// default, and persisted for cross-process coordination when SetPortFilePath is used. It does
// not keep listeners open on the reserved ports, so other non-cooperating processes may still
// bind to them after this call returns.
func GetPortReservation(count int) *PortReservation {
	if count < 1 {
		panic("osutil.GetPortReservation: count must be >= 1")
	}

	randomPortMu.Lock()
	defer randomPortMu.Unlock()

	f, cleanup := openAndLockPortFile()
	defer cleanup()
	pruneExpiredRanges()
	mergeFileRanges(f)

	for {
		base := randomPort()
		if hasRangeOverlap(allocatedRanges, base, base+count-1) || !portsAvailable(base, count) {
			continue
		}

		pr := &PortReservation{Start: base, End: base + count - 1, allocated: time.Now()}
		allocatedRanges = append(allocatedRanges, pr)
		if f != nil {
			writePortFile(f, allocatedRanges)
		}
		return pr
	}
}

// UnreservePorts releases a previously reserved port range.
func UnreservePorts(pr *PortReservation) {
	randomPortMu.Lock()
	defer randomPortMu.Unlock()

	f, cleanup := openAndLockPortFile()
	defer cleanup()

	if f != nil {
		mergeFileRanges(f)
	}

	filtered := allocatedRanges[:0]
	for _, r := range allocatedRanges {
		if r.Start == pr.Start && r.End == pr.End {
			continue
		}
		filtered = append(filtered, r)
	}
	allocatedRanges = filtered

	if f != nil {
		writePortFile(f, allocatedRanges)
	}
}
