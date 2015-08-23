///bin/true; exec /usr/bin/env go run "$0" "$@"

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
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"
)

var usage = `Usage of test.go:

go run test.go [options] [test_name ...]

If one or more test names are provided, run only those tests.
Otherwise, run all tests in test/config.json.
`

// Flags
var (
	flavor   = flag.String("flavor", "mariadb", "bootstrap flavor to run against")
	runCount = flag.Int("runs", 1, "run each test this many times")
	retryMax = flag.Int("retry", 3, "max number of retries, to detect flaky tests")
	logPass  = flag.Bool("log-pass", false, "log test output even if it passes")
	timeout  = flag.Duration("timeout", 10*time.Minute, "timeout for each test")
	pull     = flag.Bool("pull", true, "re-pull the bootstrap image, in case it's been updated")
	docker   = flag.Bool("docker", true, "run tests with Docker")

	extraArgs = flag.String("extra-args", "", "extra args to pass to each test")
)

var vtDataRoot = os.Getenv("VTDATAROOT")

// Config is the overall object serialized in test/config.json.
type Config struct {
	Tests map[string]*Test
}

// Test is an entry from the test/config.json file.
type Test struct {
	Name, File, Args, Command string

	cmd      *exec.Cmd
	runIndex int
}

// run executes a single try.
// dir is the location of the vitess repo to use.
// dataDir is the VTDATAROOT to use for this run.
// returns the combined stdout+stderr and error.
func (t *Test) run(dir, dataDir string) ([]byte, error) {
	testCmd := t.Command
	if testCmd == "" {
		testCmd = fmt.Sprintf("make build && test/%s -v --skip-build --keep-logs %s", t.File, t.Args)
		if *docker {
			// Teardown is unnecessary since Docker kills everything.
			testCmd += " --skip-teardown"
		}
		if *extraArgs != "" {
			testCmd += " " + *extraArgs
		}
	}

	if *docker {
		t.cmd = exec.Command(path.Join(dir, "docker/test/run.sh"), *flavor, testCmd)
	} else {
		t.cmd = exec.Command("bash", "-c", testCmd)
	}
	t.cmd.Dir = dir

	// Put everything in a unique dir, so we can copy and/or safely delete it.
	t.cmd.Env = updateEnv(os.Environ(), map[string]string{
		"VTDATAROOT": dataDir,
	})

	// Stop the test if it takes too long.
	done := make(chan struct{})
	timer := time.NewTimer(*timeout)
	defer timer.Stop()
	go func() {
		select {
		case <-done:
		case <-timer.C:
			t.logf("timeout exceeded")
			if t.cmd.Process != nil {
				t.cmd.Process.Signal(syscall.SIGTERM)
			}
		}
	}()

	// Run the test.
	defer close(done)
	return t.cmd.CombinedOutput()
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
	if *runCount > 1 {
		log.Printf("%v[%v/%v]: %v", t.Name, t.runIndex+1, *runCount, fmt.Sprintf(format, v...))
	} else {
		log.Printf("%v: %v", t.Name, fmt.Sprintf(format, v...))
	}
}

func main() {
	flag.Usage = func() {
		os.Stderr.WriteString(usage)
		os.Stderr.WriteString("\nOptions:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	startTime := time.Now()

	// Make output directory.
	outDir := path.Join("_test", fmt.Sprintf("%v.%v.%v", *flavor, startTime.Format("20060102-150405"), os.Getpid()))
	if err := os.MkdirAll(outDir, os.FileMode(0755)); err != nil {
		log.Fatalf("Can't create output directory: %v", err)
	}
	logFile, err := os.OpenFile(path.Join(outDir, "test.log"), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Can't create log file: %v", err)
	}
	log.SetOutput(io.MultiWriter(os.Stderr, logFile))
	log.Printf("Output directory: %v", outDir)

	// Get test configs.
	configData, err := ioutil.ReadFile("test/config.json")
	if err != nil {
		log.Fatalf("Can't read config file: %v", err)
	}
	var config Config
	if err := json.Unmarshal(configData, &config); err != nil {
		log.Fatalf("Can't parse config file: %v", err)
	}

	if *docker {
		log.Printf("Bootstrap flavor: %v", *flavor)

		// Re-pull image.
		if *docker && *pull {
			image := "vitess/bootstrap:" + *flavor
			pullTime := time.Now()
			log.Printf("Pulling %v...", image)
			cmd := exec.Command("docker", "pull", image)
			if out, err := cmd.CombinedOutput(); err != nil {
				log.Fatalf("Can't pull image: %v\n%s", err, out)
			}
			log.Printf("Image pulled in %v", time.Since(pullTime))
		}
	} else {
		if vtDataRoot == "" {
			log.Fatalf("VTDATAROOT env var must be set in -docker=false mode. Make sure to source dev.env.")
		}
	}

	// Positional args specify which tests to run.
	// If none specified, run all tests in alphabetical order.
	var tests []*Test
	if flag.NArg() > 0 {
		for _, name := range flag.Args() {
			t, ok := config.Tests[name]
			if !ok {
				log.Fatalf("Unknown test: %v", name)
			}
			t.Name = name
			tests = append(tests, t)
		}
	} else {
		var names []string
		for name := range config.Tests {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			t := config.Tests[name]
			t.Name = name
			tests = append(tests, t)
		}
	}

	// Duplicate tests.
	if *runCount > 1 {
		var dup []*Test
		for _, t := range tests {
			for i := 0; i < *runCount; i++ {
				// Make a copy, since they're pointers.
				test := *t
				test.runIndex = i
				dup = append(dup, &test)
			}
		}
		tests = dup
	}

	vtTop := "."
	tmpDir := ""
	if *docker {
		// Copy working repo to tmpDir.
		// This doesn't work outside Docker since it messes up GOROOT.
		tmpDir, err = ioutil.TempDir(os.TempDir(), "vt_")
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
		vtTop = tmpDir
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

		for _, test := range tests {
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

				// Make a unique VTDATAROOT.
				dataDir, err := ioutil.TempDir(vtDataRoot, "vt_")
				if err != nil {
					test.logf("Failed to create temporary subdir in VTDATAROOT: %v", vtDataRoot)
					failed++
					break
				}

				// Run the test.
				start := time.Now()
				output, err := test.run(vtTop, dataDir)

				// Save test output.
				if err != nil || *logPass {
					outFile := fmt.Sprintf("%v-%v.%v.log", test.Name, test.runIndex+1, try)
					test.logf("saving test output to %v", outFile)
					if fileErr := ioutil.WriteFile(path.Join(outDir, outFile), output, os.FileMode(0644)); fileErr != nil {
						test.logf("WriteFile error: %v", fileErr)
					}
				}

				// Clean up the unique VTDATAROOT.
				if err := os.RemoveAll(dataDir); err != nil {
					test.logf("WARNING: can't remove temporary VTDATAROOT: ", err)
				}

				if err != nil {
					// This try failed.
					test.logf("FAILED (try %v/%v) in %v: %v", try, *retryMax, time.Since(start), err)
					continue
				}

				if try == 1 {
					// Passed on the first try.
					test.logf("PASSED in %v", time.Since(start))
					passed++
				} else {
					// Passed, but not on the first try.
					test.logf("FLAKY (1/%v passed in %v)", try, time.Since(start))
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
		for _, t := range tests {
			t.stop()
		}
	case <-done:
	}

	// Clean up temp dir.
	if tmpDir != "" {
		log.Printf("Removing temp dir %v", tmpDir)
		if err := os.RemoveAll(tmpDir); err != nil {
			log.Printf("Failed to remove temp dir: %v", err)
		}
	}

	// Print stats.
	skipped := len(tests) - passed - flaky - failed
	log.Printf("%v PASSED, %v FLAKY, %v FAILED, %v SKIPPED", passed, flaky, failed, skipped)
	log.Printf("Total time: %v", time.Since(startTime))

	if failed > 0 || skipped > 0 {
		os.Exit(1)
	}
}

func updateEnv(orig []string, updates map[string]string) []string {
	var env []string
	for _, v := range orig {
		parts := strings.SplitN(v, "=", 2)
		if _, ok := updates[parts[0]]; !ok {
			env = append(env, v)
		}
	}
	for k, v := range updates {
		env = append(env, k+"="+v)
	}
	return env
}
