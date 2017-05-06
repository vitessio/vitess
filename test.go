///bin/true; exec /usr/bin/env go run "$0" "$@"

/*
Copyright 2017 Google Inc.

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
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var usage = `Usage of test.go:

go run test.go [options] [test_name ...] [-- extra-py-test-args]

If one or more test names are provided, run only those tests.
Otherwise, run all tests in test/config.json.

To pass extra args to Python tests (test/*.py), terminate the
list of test names with -- and then add them at the end.

For example:
  go run test.go test1 test2 -- --topo-server-flavor=etcd
`

// List of flavors for which a bootstrap Docker image is available.
const flavors = "mariadb,mysql56,mysql57,percona,percona57"

// Flags
var (
	flavor   = flag.String("flavor", "mysql57", "comma-separated bootstrap flavor(s) to run against (when using Docker mode). Available flavors: all,"+flavors)
	runCount = flag.Int("runs", 1, "run each test this many times")
	retryMax = flag.Int("retry", 3, "max number of retries, to detect flaky tests")
	logPass  = flag.Bool("log-pass", false, "log test output even if it passes")
	timeout  = flag.Duration("timeout", 10*time.Minute, "timeout for each test")
	pull     = flag.Bool("pull", true, "re-pull the bootstrap image, in case it's been updated")
	docker   = flag.Bool("docker", true, "run tests with Docker")
	shard    = flag.Int("shard", -1, "if N>=0, run the tests whose Shard field matches N")
	tag      = flag.String("tag", "", "if provided, only run tests with the given tag. Can't be combined with -shard or explicit test list")
	exclude  = flag.String("exclude", "", "if provided, exclude tests containing any of the given tags (comma delimited)")
	reshard  = flag.Int("rebalance", 0, "if N>0, check the stats and group tests into N similarly-sized bins by average run time")
	keepData = flag.Bool("keep-data", false, "don't delete the per-test VTDATAROOT subfolders")
	printLog = flag.Bool("print-log", false, "print the log of each failed test (or all tests if -log-pass) to the console")
	follow   = flag.Bool("follow", false, "print test output as it runs, instead of waiting to see if it passes or fails")
	parallel = flag.Int("parallel", 1, "number of tests to run in parallel")

	remoteStats = flag.String("remote-stats", "", "url to send remote stats")
)

var (
	vtDataRoot = os.Getenv("VTDATAROOT")

	extraArgs []string
)

const (
	statsFileName  = "test/stats.json"
	configFileName = "test/config.json"
)

// Config is the overall object serialized in test/config.json.
type Config struct {
	Tests map[string]*Test
}

// Test is an entry from the test/config.json file.
type Test struct {
	File          string
	Args, Command []string

	// Manual means it won't be run unless explicitly specified.
	Manual bool

	// Shard is used to split tests among workers.
	Shard int

	// RetryMax is the maximum number of times a test will be retried.
	// If 0, flag *retryMax is used.
	RetryMax int

	// Tags is a list of tags that can be used to filter tests.
	Tags []string

	name     string
	flavor   string
	runIndex int

	pass, fail int
}

func (t *Test) hasTag(want string) bool {
	for _, got := range t.Tags {
		if got == want {
			return true
		}
	}
	return false
}

func (t *Test) hasAnyTag(want []string) bool {
	for _, tag := range want {
		if t.hasTag(tag) {
			return true
		}
	}
	return false
}

// run executes a single try.
// dir is the location of the vitess repo to use.
// dataDir is the VTDATAROOT to use for this run.
// returns the combined stdout+stderr and error.
func (t *Test) run(dir, dataDir string) ([]byte, error) {
	testCmd := t.Command
	if len(testCmd) == 0 {
		testCmd = []string{"test/" + t.File, "-v", "--skip-build", "--keep-logs"}
		testCmd = append(testCmd, t.Args...)
		testCmd = append(testCmd, extraArgs...)
		if *docker {
			// Teardown is unnecessary since Docker kills everything.
			testCmd = append(testCmd, "--skip-teardown")
		}
	}

	var cmd *exec.Cmd
	if *docker {
		cmd = exec.Command(path.Join(dir, "docker/test/run.sh"), t.flavor, "make build && "+strings.Join(testCmd, " "))
	} else {
		cmd = exec.Command(testCmd[0], testCmd[1:]...)
	}
	cmd.Dir = dir

	// Put everything in a unique dir, so we can copy and/or safely delete it.
	// Also try to make them use different port ranges
	// to mitigate failures due to zombie processes.
	cmd.Env = updateEnv(os.Environ(), map[string]string{
		"VTDATAROOT":  dataDir,
		"VTPORTSTART": strconv.FormatInt(int64(getPortStart(100)), 10),
	})

	// Capture test output.
	buf := &bytes.Buffer{}
	cmd.Stdout = buf
	if *follow {
		cmd.Stdout = io.MultiWriter(cmd.Stdout, os.Stdout)
	}
	cmd.Stderr = cmd.Stdout

	// Run the test.
	done := make(chan error, 1)
	go func() {
		done <- cmd.Run()
	}()

	// Wait for it to finish.
	var runErr error
	timer := time.NewTimer(*timeout)
	defer timer.Stop()
	select {
	case runErr = <-done:
		if runErr == nil {
			t.pass++
		} else {
			t.fail++
		}
	case <-timer.C:
		t.logf("timeout exceeded")
		cmd.Process.Signal(syscall.SIGINT)
		t.fail++
		runErr = <-done
	}
	return buf.Bytes(), runErr
}

func (t *Test) logf(format string, v ...interface{}) {
	if *runCount > 1 {
		log.Printf("%v.%v[%v/%v]: %v", t.flavor, t.name, t.runIndex+1, *runCount, fmt.Sprintf(format, v...))
	} else {
		log.Printf("%v.%v: %v", t.flavor, t.name, fmt.Sprintf(format, v...))
	}
}

func main() {
	flag.Usage = func() {
		os.Stderr.WriteString(usage)
		os.Stderr.WriteString("\nOptions:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	// Sanity checks.
	if *docker {
		if *flavor == "all" {
			*flavor = flavors
		}
		if *flavor == "" {
			log.Fatalf("Must provide at least one -flavor when using -docker mode. Available flavors: all,%v", flavors)
		}
	}
	if *parallel < 1 {
		log.Fatalf("Invalid -parallel value: %v", *parallel)
	}
	if *parallel > 1 && !*docker {
		log.Fatalf("Can't use -parallel value > 1 when -docker=false")
	}

	startTime := time.Now()

	// Make output directory.
	outDir := path.Join("_test", fmt.Sprintf("%v.%v", startTime.Format("20060102-150405"), os.Getpid()))
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
	configData, err := ioutil.ReadFile(configFileName)
	if err != nil {
		log.Fatalf("Can't read config file: %v", err)
	}
	var config Config
	if err := json.Unmarshal(configData, &config); err != nil {
		log.Fatalf("Can't parse config file: %v", err)
	}

	// Resharding.
	if *reshard > 0 {
		if err := reshardTests(&config, *reshard); err != nil {
			log.Fatalf("resharding error: %v", err)
		}
		log.Printf("Saving updated config...")
		data, err := json.MarshalIndent(config, "", "\t")
		if err != nil {
			log.Fatalf("can't save new config: %v", err)
		}
		if err := ioutil.WriteFile(configFileName, data, 0644); err != nil {
			log.Fatalf("can't write new config: %v", err)
		}
		return
	}

	flavors := []string{"local"}

	if *docker {
		log.Printf("Bootstrap flavor(s): %v", *flavor)

		flavors = strings.Split(*flavor, ",")

		// Re-pull image(s).
		if *pull {
			var wg sync.WaitGroup
			for _, flavor := range flavors {
				wg.Add(1)
				go func(flavor string) {
					defer wg.Done()
					image := "vitess/bootstrap:" + flavor
					pullTime := time.Now()
					log.Printf("Pulling %v...", image)
					cmd := exec.Command("docker", "pull", image)
					if out, err := cmd.CombinedOutput(); err != nil {
						log.Fatalf("Can't pull image %v: %v\n%s", image, err, out)
					}
					log.Printf("Image %v pulled in %v", image, time.Since(pullTime))
				}(flavor)
			}
			wg.Wait()
		}
	} else {
		if vtDataRoot == "" {
			log.Fatalf("VTDATAROOT env var must be set in -docker=false mode. Make sure to source dev.env.")
		}
	}

	// Pick the tests to run.
	var testArgs []string
	testArgs, extraArgs = splitArgs(flag.Args(), "--")
	tests := selectedTests(testArgs, &config)

	// Duplicate tests for run count.
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

	// Duplicate tests for flavors.
	var dup []*Test
	for _, flavor := range flavors {
		for _, t := range tests {
			test := *t
			test.flavor = flavor
			dup = append(dup, &test)
		}
	}
	tests = dup

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
	} else {
		// Since we're sharing the working dir, do the build once for all tests.
		log.Printf("Running make build...")
		if out, err := exec.Command("make", "build").CombinedOutput(); err != nil {
			log.Fatalf("make build failed: %v\n%s", err, out)
		}
	}

	// Keep stats for the overall run.
	var mu sync.Mutex
	failed := 0
	passed := 0
	flaky := 0

	// Listen for signals.
	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT)

	// Run tests.
	stop := make(chan struct{}) // Close this to tell the runners to stop.
	done := make(chan struct{}) // This gets closed when all runners have stopped.
	next := make(chan *Test)    // The next test to run.
	var wg sync.WaitGroup

	// Send all tests into the channel.
	go func() {
		for _, test := range tests {
			next <- test
		}
		close(next)
	}()

	// Start the requested number of parallel runners.
	for i := 0; i < *parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for test := range next {
				tryMax := *retryMax
				if test.RetryMax != 0 {
					tryMax = test.RetryMax
				}
				for try := 1; ; try++ {
					select {
					case <-stop:
						test.logf("cancelled")
						return
					default:
					}

					if try > tryMax {
						// Every try failed.
						test.logf("retry limit exceeded")
						mu.Lock()
						failed++
						mu.Unlock()
						break
					}

					test.logf("running (try %v/%v)...", try, tryMax)

					// Make a unique VTDATAROOT.
					dataDir, err := ioutil.TempDir(vtDataRoot, "vt_")
					if err != nil {
						test.logf("Failed to create temporary subdir in VTDATAROOT: %v", vtDataRoot)
						mu.Lock()
						failed++
						mu.Unlock()
						break
					}

					// Run the test.
					start := time.Now()
					output, err := test.run(vtTop, dataDir)
					duration := time.Since(start)

					// Save/print test output.
					if err != nil || *logPass {
						if *printLog && !*follow {
							test.logf("%s\n", output)
						}
						outFile := fmt.Sprintf("%v.%v-%v.%v.log", test.flavor, test.name, test.runIndex+1, try)
						outFilePath := path.Join(outDir, outFile)
						test.logf("saving test output to %v", outFilePath)
						if fileErr := ioutil.WriteFile(outFilePath, output, os.FileMode(0644)); fileErr != nil {
							test.logf("WriteFile error: %v", fileErr)
						}
					}

					// Clean up the unique VTDATAROOT.
					if !*keepData {
						if err := os.RemoveAll(dataDir); err != nil {
							test.logf("WARNING: can't remove temporary VTDATAROOT: %v", err)
						}
					}

					if err != nil {
						// This try failed.
						test.logf("FAILED (try %v/%v) in %v: %v", try, tryMax, duration, err)
						mu.Lock()
						testFailed(test.name)
						mu.Unlock()
						continue
					}

					mu.Lock()
					testPassed(test.name, duration)

					if try == 1 {
						// Passed on the first try.
						test.logf("PASSED in %v", duration)
						passed++
					} else {
						// Passed, but not on the first try.
						test.logf("FLAKY (1/%v passed in %v)", try, duration)
						flaky++
						testFlaked(test.name, try)
					}
					mu.Unlock()
					break
				}
			}
		}()
	}

	// Close the done channel when all the runners stop.
	// This lets us select on wg.Wait().
	go func() {
		wg.Wait()
		close(done)
	}()

	// Stop the loop and kill child processes if we get a signal.
	select {
	case <-sigchan:
		log.Printf("interrupted: skip remaining tests and wait for current test to tear down")
		signal.Stop(sigchan)
		// Stop the test loop and wait for it to exit.
		// Running tests already get the SIGINT themselves.
		// We mustn't send it again, or it'll abort the teardown process too early.
		close(stop)
		<-done
	case <-done:
	}

	// Clean up temp dir.
	if tmpDir != "" {
		log.Printf("Removing temp dir %v", tmpDir)
		if err := os.RemoveAll(tmpDir); err != nil {
			log.Printf("Failed to remove temp dir: %v", err)
		}
	}

	// Print summary.
	log.Printf(strings.Repeat("=", 60))
	for _, t := range tests {
		tname := t.flavor + "." + t.name
		switch {
		case t.pass > 0 && t.fail == 0:
			log.Printf("%-40s\tPASS", tname)
		case t.pass > 0 && t.fail > 0:
			log.Printf("%-40s\tFLAKY (%v/%v failed)", tname, t.fail, t.pass+t.fail)
		case t.pass == 0 && t.fail > 0:
			log.Printf("%-40s\tFAIL (%v tries)", tname, t.fail)
		case t.pass == 0 && t.fail == 0:
			log.Printf("%-40s\tSKIPPED", tname)
		}
	}
	log.Printf(strings.Repeat("=", 60))
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

type Stats struct {
	TestStats map[string]TestStats
}

type TestStats struct {
	Pass, Fail, Flake int
	PassTime          time.Duration

	name string
}

func sendStats(values url.Values) {
	if *remoteStats != "" {
		log.Printf("Sending remote stats to %v", *remoteStats)
		if _, err := http.PostForm(*remoteStats, values); err != nil {
			log.Printf("Can't send remote stats: %v", err)
		}
	}
}

func testPassed(name string, passTime time.Duration) {
	sendStats(url.Values{
		"test":     {name},
		"result":   {"pass"},
		"duration": {passTime.String()},
	})
	updateTestStats(name, func(ts *TestStats) {
		totalTime := int64(ts.PassTime)*int64(ts.Pass) + int64(passTime)
		ts.Pass++
		ts.PassTime = time.Duration(totalTime / int64(ts.Pass))
	})
}

func testFailed(name string) {
	sendStats(url.Values{
		"test":   {name},
		"result": {"fail"},
	})
	updateTestStats(name, func(ts *TestStats) {
		ts.Fail++
	})
}

func testFlaked(name string, try int) {
	sendStats(url.Values{
		"test":   {name},
		"result": {"flake"},
		"try":    {strconv.FormatInt(int64(try), 10)},
	})
	updateTestStats(name, func(ts *TestStats) {
		ts.Flake += try - 1
	})
}

func updateTestStats(name string, update func(*TestStats)) {
	var stats Stats

	data, err := ioutil.ReadFile(statsFileName)
	if err != nil {
		log.Print("Can't read stats file, starting new one.")
	} else {
		if err := json.Unmarshal(data, &stats); err != nil {
			log.Printf("Can't parse stats file: %v", err)
			return
		}
	}

	if stats.TestStats == nil {
		stats.TestStats = make(map[string]TestStats)
	}
	ts := stats.TestStats[name]
	update(&ts)
	stats.TestStats[name] = ts

	data, err = json.MarshalIndent(stats, "", "\t")
	if err != nil {
		log.Printf("Can't encode stats file: %v", err)
		return
	}
	if err := ioutil.WriteFile(statsFileName, data, 0644); err != nil {
		log.Printf("Can't write stats file: %v", err)
	}
}

func reshardTests(config *Config, numShards int) error {
	var stats Stats

	var data []byte
	if *remoteStats != "" {
		log.Printf("Using remote stats for resharding: %v", *remoteStats)
		resp, err := http.Get(*remoteStats)
		if err != nil {
			return err
		}
		if data, err = ioutil.ReadAll(resp.Body); err != nil {
			return err
		}
	} else {
		var err error
		data, err = ioutil.ReadFile(statsFileName)
		if err != nil {
			return errors.New("can't read stats file")
		}
	}

	if err := json.Unmarshal(data, &stats); err != nil {
		return fmt.Errorf("can't parse stats file: %v", err)
	}

	// Sort tests by PassTime.
	var tests []TestStats
	var totalTime int64
	for name, test := range stats.TestStats {
		test.name = name
		tests = append(tests, test)
		totalTime += int64(test.PassTime)
	}
	sort.Sort(ByPassTime(tests))

	// Group into shards.
	max := totalTime / int64(numShards)
	shards := make([][]TestStats, numShards)
	sums := make([]int64, numShards)
	// First pass: greedy approximation.
firstPass:
	for len(tests) > 0 {
		v := int64(tests[0].PassTime)

		for n := range shards {
			if sums[n]+v < max {
				shards[n] = append(shards[n], tests[0])
				sums[n] += v
				tests = tests[1:]
				continue firstPass
			}
		}
		// None of the bins has room. Go to second pass.
		break
	}
	// Second pass: distribute the remainder.
	for len(tests) > 0 {
		nmin := 0
		min := sums[0]

		for n := range sums {
			if sums[n] < min {
				nmin = n
				min = sums[n]
			}
		}

		shards[nmin] = append(shards[nmin], tests[0])
		sums[nmin] += int64(tests[0].PassTime)
		tests = tests[1:]
	}

	// Update config and print results.
	for i, tests := range shards {
		for _, t := range tests {
			ct, ok := config.Tests[t.name]
			if !ok {
				log.Printf("WARNING: skipping unknown test: %v", t.name)
				continue
			}
			ct.Shard = i
			if ct.Args == nil {
				ct.Args = []string{}
			}
			if ct.Command == nil {
				ct.Command = []string{}
			}
			if ct.Tags == nil {
				ct.Tags = []string{}
			}
			log.Printf("% 32v:\t%v\n", t.name, t.PassTime)
		}
		log.Printf("Shard %v total: %v\n", i, time.Duration(sums[i]))
	}

	return nil
}

type ByPassTime []TestStats

func (a ByPassTime) Len() int           { return len(a) }
func (a ByPassTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByPassTime) Less(i, j int) bool { return a[i].PassTime > a[j].PassTime }

func getTestsSorted(names []string, testMap map[string]*Test) []*Test {
	sort.Strings(names)
	var tests []*Test
	for _, name := range names {
		t := testMap[name]
		t.name = name
		tests = append(tests, t)
	}
	return tests
}

func selectedTests(args []string, config *Config) []*Test {
	var tests []*Test
	excluded_tests := strings.Split(*exclude, ",")
	if *shard >= 0 {
		// Run the tests in a given shard.
		// This can be combined with positional args.
		var names []string
		for name, t := range config.Tests {
			if t.Shard == *shard && !t.Manual && (*exclude == "" || !t.hasAnyTag(excluded_tests)) {
				t.name = name
				names = append(names, name)
			}
		}
		tests = getTestsSorted(names, config.Tests)
	}
	if len(args) > 0 {
		// Positional args for manual selection.
		for _, name := range args {
			t, ok := config.Tests[name]
			if !ok {
				log.Fatalf("Unknown test: %v", name)
			}
			t.name = name
			tests = append(tests, t)
		}
	}
	if len(args) == 0 && *shard < 0 {
		// Run all tests.
		var names []string
		for name, t := range config.Tests {
			if !t.Manual && (*tag == "" || t.hasTag(*tag)) && (*exclude == "" || !t.hasAnyTag(excluded_tests)) {
				names = append(names, name)
			}
		}
		tests = getTestsSorted(names, config.Tests)
	}
	return tests
}

var (
	port      = 16000
	portMutex sync.Mutex
)

func getPortStart(size int) int {
	portMutex.Lock()
	defer portMutex.Unlock()

	start := port
	port += size
	return start
}

// splitArgs splits a list of args at the first appearance of tok.
func splitArgs(all []string, tok string) (args, extraArgs []string) {
	extra := false
	for _, arg := range all {
		if extra {
			extraArgs = append(extraArgs, arg)
			continue
		}
		if arg == tok {
			extra = true
			continue
		}
		args = append(args, arg)
	}
	return
}
