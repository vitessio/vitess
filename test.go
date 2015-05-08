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

// This Go script shouldn't rely on any packages that aren't in the standard
// library, since that would require the user to bootstrap before running it.
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

// Flags
var (
	flavor   = flag.String("flavor", "mariadb", "bootstrap flavor to run against")
	retryMax = flag.Int("retry", 3, "max number of retries, to detect flaky tests")
	logPass  = flag.Bool("log-pass", false, "log test output even if it passes")
	timeout  = flag.Duration("timeout", 10*time.Minute, "timeout for each test")
)

// Config is the overall object serialized in test/config.json.
type Config struct {
	Tests []*Test
}

// Test is an entry from the test/config.json file.
type Test struct {
	Name, File, Args string

	cmd *exec.Cmd
}

// run executes a single try.
// dir is the location of the vitess repo to use.
func (t *Test) run(dir string) error {
	// Teardown is unnecessary since Docker kills everything.
	testCmd := fmt.Sprintf("make build && test/%s -v --skip-teardown %s", t.File, t.Args)
	dockerCmd := exec.Command(path.Join(dir, "docker/test/run.sh"), *flavor, testCmd)
	dockerCmd.Dir = dir
	t.cmd = dockerCmd

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

	// Run the test.
	output, err := dockerCmd.CombinedOutput()
	close(done)

	// Save test output.
	if err != nil || *logPass {
		outDir := path.Join("_test", *flavor)
		outFile := path.Join(outDir, t.Name+".log")
		t.logf("saving test output to %v", outFile)
		if dirErr := os.MkdirAll(outDir, os.FileMode(0755)); dirErr != nil {
			t.logf("Mkdir error: %v", dirErr)
		}
		if fileErr := ioutil.WriteFile(outFile, output, os.FileMode(0644)); fileErr != nil {
			t.logf("WriteFile error: %v", fileErr)
		}
	}
	return err
}

// stop will terminate the test if it's running.
// If the test is not running, it's a no-op.
func (t *Test) stop() {
	if cmd := t.cmd; cmd != nil {
		if proc := cmd.Process; proc != nil {
			proc.Signal(syscall.SIGTERM)
		}
	}
}

func (t *Test) logf(format string, v ...interface{}) {
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
	log.Printf("Bootstrap flavor: %v", *flavor)

	// Copy working repo to tmpDir.
	tmpDir, err := ioutil.TempDir(os.TempDir(), "vt_")
	if err != nil {
		log.Fatalf("Can't create temp dir in %v", os.TempDir())
	}
	log.Printf("Copying working repo to temp dir %v", tmpDir)
	if out, err := exec.Command("cp", "-R", ".", tmpDir).CombinedOutput(); err != nil {
		log.Fatalf("Can't copy working repo to temp dir %v: %v: %s", tmpDir, err, out)
	}
	// The temp copy needs permissive access so the Docker user can read it.
	if out, err := exec.Command("chmod", "-R", "go=u", tmpDir).CombinedOutput(); err != nil {
		log.Printf("Can't set permissions on temp dir %v: %v: %s", tmpDir, err, out)
	}

	// Keep stats.
	failed := 0
	passed := 0
	flaky := 0

	// Listen for signals.
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Run tests.
	stop := make(chan struct{}) // Close this to tell the loop to stop.
	done := make(chan struct{}) // The loop closes this when it has stopped.
	go func() {
		defer func() {
			signal.Stop(sigchan)
			close(done)
		}()

		for _, test := range config.Tests {
			if test.Name == "" {
				test.Name = strings.TrimSuffix(test.File, ".py")
			}

			for try := 1; ; try++ {
				select {
				case <-stop:
					test.logf("cancelled")
					return
				default:
				}

				if try > *retryMax {
					// Every try failed.
					test.logf("retry limit exceeded")
					failed++
					break
				}

				test.logf("running (try %v/%v)...", try, *retryMax)
				start := time.Now()
				if err := test.run(tmpDir); err != nil {
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
	}()

	// Stop the loop and kill child processes if we get a signal.
	select {
	case <-sigchan:
		log.Printf("received signal, quitting")
		// Stop the test loop and wait for it to quit.
		close(stop)
		<-done
		// Terminate all existing tests.
		for _, t := range config.Tests {
			t.stop()
		}
	case <-done:
	}

	// Clean up temp dir.
	log.Printf("Removing temp dir %v", tmpDir)
	if err := os.RemoveAll(tmpDir); err != nil {
		log.Printf("Failed to remove temp dir: %v", err)
	}

	// Print stats.
	skipped := len(config.Tests) - passed - flaky - failed
	log.Printf("%v PASSED, %v FLAKY, %v FAILED, %v SKIPPED", passed, flaky, failed, skipped)

	if failed > 0 || skipped > 0 {
		os.Exit(1)
	}
}
