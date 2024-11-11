/*
Copyright 2024 The Vitess Authors.

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
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

// LoadAvg returns the past 1 minute system load average. This works on linux and darwin systems.
// On other systems, it returns 0 with no error.
func LoadAvg() (float64, error) {
	switch runtime.GOOS {
	case "linux":
		content, err := os.ReadFile("/proc/loadavg")
		if err != nil {
			return 0, err
		}
		return parseLoadAvg(string(content))
	case "darwin":
		cmd := exec.Command("sysctl", "-n", "vm.loadavg")
		// Sample output: `{ 2.83 3.01 3.36 }`
		output, err := cmd.CombinedOutput()
		if err != nil {
			return 0, err
		}
		if len(output) < 1 {
			return 0, fmt.Errorf("unexpected sysctl output: %q", output)
		}
		output = output[1:] // Remove the leading `{ `
		return parseLoadAvg(string(output))
	default:
		return 0, nil
	}
}

// parseLoadAvg parses the load average from the content of /proc/loadavg or sysctl output.
// Input such as "1.00 0.99 0.98 1/1 1", "2.83 3.01 3.36"
func parseLoadAvg(content string) (float64, error) {
	fields := strings.Fields(content)
	if len(fields) == 0 {
		return 0, fmt.Errorf("unexpected loadavg content: %s", content)
	}
	return strconv.ParseFloat(fields[0], 64)
}
