/*
Copyright 2019 The Vitess Authors.

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

package logutil

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

func TestConsoleLogger(t *testing.T) {
	testConsoleLogger(t, false, "TestConsoleLogger")
}

func TestTeeConsoleLogger(t *testing.T) {
	testConsoleLogger(t, true, "TestTeeConsoleLogger")
}

func testConsoleLogger(t *testing.T, tee bool, entrypoint string) {
	if os.Getenv("TEST_CONSOLE_LOGGER") == "1" {
		// Generate output in subprocess.
		var logger Logger
		if tee {
			logger = NewTeeLogger(NewConsoleLogger(), NewMemoryLogger())
		} else {
			logger = NewConsoleLogger()
		}
		// Add 'tee' to the output to make sure we've
		// called the right method in the subprocess.
		logger.Infof("info %v %v", 1, tee)
		logger.Warningf("warning %v %v", 2, tee)
		logger.Errorf("error %v %v", 3, tee)
		return
	}

	// Run subprocess and collect console output.
	cmd := exec.Command(os.Args[0], "-test.run=^"+entrypoint+"$", "-logtostderr")
	cmd.Env = append(os.Environ(), "TEST_CONSOLE_LOGGER=1")
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("cmd.StderrPipe() error: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("cmd.Start() error: %v", err)
	}
	out, err := ioutil.ReadAll(stderr)
	if err != nil {
		t.Fatalf("ioutil.ReadAll(sterr) error: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("cmd.Wait() error: %v", err)
	}

	// Check output. Filter out entries that are not from console_logger_test.go
	lines := strings.Split(string(out), "\n")
	gotlines := []string{}
	for _, line := range lines {
		if strings.Contains(line, "console_logger_test.go") {
			gotlines = append(gotlines, line)
		}
	}
	wantlines := []string{
		fmt.Sprintf("^I.*info 1 %v$", tee),
		fmt.Sprintf("^W.*warning 2 %v$", tee),
		fmt.Sprintf("^E.*error 3 %v$", tee),
	}
	for i, want := range wantlines {
		got := gotlines[i]
		match, err := regexp.MatchString(want, got)
		if err != nil {
			t.Errorf("regexp.MatchString error: %v", err)
		}
		if !match {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}
