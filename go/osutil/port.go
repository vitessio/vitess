/*
Copyright 2025 The Vitess Authors.

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
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// portFileTimeout is how long port reservations remain valid in the port file.
	// Acts as a safety net for processes that crash without calling UnreservePorts.
	portFileTimeout = 5 * time.Minute
)

// PortReservation represents an inclusive range of reserved TCP ports.
type PortReservation struct {
	Start, End int
}

type portAllocation struct {
	PortReservation
	allocated time.Time
}

var (
	randomPortMu    sync.Mutex
	allocatedRanges []portAllocation
	portFilePath    string
)

// SetPortFilePath enables file-based port tracking with locking for cross-process coordination.
func SetPortFilePath(path string) {
	randomPortMu.Lock()
	defer randomPortMu.Unlock()
	portFilePath = path
}

func rangeOverlaps(ranges []portAllocation, from, to int) bool {
	for _, r := range ranges {
		if from <= r.End && to >= r.Start {
			return true
		}
	}
	return false
}

func readPortFile(f *os.File) []portAllocation {
	var ranges []portAllocation
	now := time.Now()
	if _, err := f.Seek(0, 0); err != nil {
		return nil
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 3)
		if len(parts) != 3 {
			continue
		}
		from, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}
		to, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}
		ts, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			continue
		}
		allocated := time.Unix(ts, 0)
		if now.Sub(allocated) > portFileTimeout {
			continue
		}
		ranges = append(ranges, portAllocation{
			PortReservation: PortReservation{Start: from, End: to},
			allocated:       allocated,
		})
	}
	return ranges
}

func writePortFile(f *os.File, ranges []portAllocation) {
	_ = f.Truncate(0)
	_, _ = f.Seek(0, 0)
	for _, r := range ranges {
		fmt.Fprintf(f, "%d %d %d\n", r.Start, r.End, r.allocated.Unix())
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

// mergeFileRanges merges port file ranges into the in-memory list.
func mergeFileRanges(f *os.File) {
	if f == nil {
		return
	}
	for _, r := range readPortFile(f) {
		if !rangeOverlaps(allocatedRanges, r.Start, r.End) {
			allocatedRanges = append(allocatedRanges, r)
		}
	}
}

// GetPortReservation reserves count consecutive available TCP ports on 127.0.0.1.
// Ranges are tracked in-memory by default; call SetPortFilePath for cross-process coordination.
func GetPortReservation(count int) *PortReservation {
	if count < 1 {
		panic("osutil.GetPortReservation: count must be >= 1")
	}

	randomPortMu.Lock()
	defer randomPortMu.Unlock()

	f, cleanup := openAndLockPortFile()
	defer cleanup()

	mergeFileRanges(f)

	for {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			panic(fmt.Sprintf("osutil.GetPortReservation: failed to listen: %v", err))
		}
		basePort := l.Addr().(*net.TCPAddr).Port
		l.Close()

		if rangeOverlaps(allocatedRanges, basePort, basePort+count-1) {
			continue
		}

		allAvailable := true
		for i := 1; i < count; i++ {
			ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(basePort+i)))
			if err != nil {
				allAvailable = false
				break
			}
			ln.Close()
		}
		if !allAvailable {
			continue
		}

		pr := &PortReservation{Start: basePort, End: basePort + count - 1}
		allocatedRanges = append(allocatedRanges, portAllocation{
			PortReservation: *pr,
			allocated:       time.Now(),
		})

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
