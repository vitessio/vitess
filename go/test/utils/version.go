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

package utils

import (
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"testing"
)

var versionRegex = regexp.MustCompile(`Version: ([0-9]+)\.([0-9]+)\.([0-9]+)`)

// GetMajorVersion gets the major version number from a binary by running --version
func GetMajorVersion(binaryName string) (int, error) {
	version, err := exec.Command(binaryName, "--version").Output()
	if err != nil {
		return 0, err
	}
	v := versionRegex.FindStringSubmatch(string(version))
	if len(v) != 4 {
		return 0, fmt.Errorf("could not parse server version from: %s", version)
	}
	return strconv.Atoi(v[1])
}

// SkipIfBinaryIsBelowVersion skips the given test if the binary's major version is below majorVersion.
func SkipIfBinaryIsBelowVersion(t *testing.T, majorVersion int, binary string) {
	version, err := GetMajorVersion(binary)
	if err != nil {
		return
	}
	if version < majorVersion {
		t.Skip("Current version of ", binary, ": v", version, ", expected version >= v", majorVersion)
	}
}
