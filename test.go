// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
test.go is a "Go script" for running Vitess tests. It runs each test in its own
Docker container for hermeticity and (potentially) parallelism. If a test fails,
this script will save the output in _test/ and continue with other tests.

Before using it, you should have Docker 1.5+ installed, and have your user in
the group that lets you run the docker command without sudo. The first time you
run against a given flavor, it may take some time for the corresponding
bootstrap image (vitess/bootstrap:<flavor>) to be downloaded.

It is meant to be run from the Vitess root, like so:
  ~/src/github.com/youtube/vitess$ go run test.go [args]

For a list of options, run:
  $ go run test.go --help
*/
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"
)

var (
	flavor   = flag.String("flavor", "mariadb", "bootstrap flavor to run against")
	retryMax = flag.Int("retry", 3, "max number of retries, to detect flaky tests")
	logPass  = flag.Bool("log-pass", false, "log test output even if it passes")
	timeout  = flag.Duration("timeout", 10*time.Minute, "timeout for each test")
)

// Config is the overall object serialized in test/config.json.
type Config struct {
	Tests []Test
}

// Test is an entry from the test/config.json file.
type Test struct {
	Name, File, Args string
}

// run executes a single try.
func (t Test) run() error {
	testCmd := fmt.Sprintf("make build && test/%s %s", t.File, t.Args)
	dockerCmd := exec.Command("docker/test/run.sh", *flavor, testCmd)

	// Kill child process if we get a signal.
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		if _, ok := <-sigchan; ok {
			if dockerCmd.Process != nil {
				dockerCmd.Process.Signal(syscall.SIGTERM)
			}
			log.Fatalf("received signal, quitting")
		}
	}()

	// Stop the test if it takes too long.
	done := make(chan struct{})
	timer := time.NewTimer(*timeout)
	defer timer.Stop()
	go func() {
		select {
		case <-done:
		case <-timer.C:
			t.logf("timeout exceeded")
			if dockerCmd.Process != nil {
				dockerCmd.Process.Signal(syscall.SIGTERM)
			}
		}
	}()

	output, err := dockerCmd.CombinedOutput()
	close(done)
	signal.Stop(sigchan)
	close(sigchan)

	if err != nil || *logPass {
		outFile := path.Join("_test", t.Name+".log")
		t.logf("saving test output to %v", outFile)
		if dirErr := os.MkdirAll("_test", os.FileMode(0755)); dirErr != nil {
			t.logf("Mkdir error: %v", dirErr)
		}
		if fileErr := ioutil.WriteFile(outFile, output, os.FileMode(0644)); fileErr != nil {
			t.logf("WriteFile error: %v", fileErr)
		}
	}
	return err
}

func (t Test) logf(format string, v ...interface{}) {
	log.Printf("%v: %v", t.Name, fmt.Sprintf(format, v...))
}

func main() {
	flag.Parse()

	// Get test configs.
	configData, err := ioutil.ReadFile("test/config.json")
	if err != nil {
		log.Fatalf("Can't read config file: %v", err)
	}
	var config Config
	if err := json.Unmarshal(configData, &config); err != nil {
		log.Fatalf("Can't parse config file: %v", err)
	}

	// Keep stats.
	failed := 0
	passed := 0
	flaky := 0

	// Run tests.
	for _, test := range config.Tests {
		if test.Name == "" {
			test.Name = strings.TrimSuffix(test.File, ".py")
		}

		for try := 1; ; try++ {
			if try > *retryMax {
				// Every try failed.
				test.logf("retry limit exceeded")
				failed++
				break
			}

			test.logf("running (try %v/%v)...", try, *retryMax)
			start := time.Now()
			if err := test.run(); err != nil {
				// This try failed.
				test.logf("FAILED (try %v/%v): %v", try, *retryMax, err)
				continue
			}

			if try == 1 {
				// Passed on the first try.
				test.logf("PASSED in %v", time.Since(start))
				passed++
			} else {
				// Passed, but not on the first try.
				test.logf("FLAKY (1/%v passed)", try)
				flaky++
			}
			break
		}
	}

	// Print stats.
	log.Printf("%v PASSED, %v FLAKY, %v FAILED", passed, flaky, failed)
}
