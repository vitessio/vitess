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
	"strconv"
	"strings"
)

// parseLoadAvg parses the load average from the content of /proc/loadavg or sysctl output.
// Input such as "1.00 0.99 0.98 1/1 1", "2.83 3.01 3.36"
func parseLoadAvg(content string) (float64, error) {
	fields := strings.Fields(content)
	if len(fields) == 0 {
		return 0, fmt.Errorf("unexpected loadavg content: %s", content)
	}
	return strconv.ParseFloat(fields[0], 64)
}
