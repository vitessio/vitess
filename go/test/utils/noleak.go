package utils

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"vitess.io/vitess/go/vt/log"
)

// EnsureNoLeaks checks for goroutine and socket leaks and fails the test if any are found.
func EnsureNoLeaks(t testing.TB) {
	if t.Failed() {
		return
	}
	if err := ensureNoLeaks(); err != nil {
		t.Fatal(err)
	}
}

// GetLeaks checks for goroutine and socket leaks and returns an error if any are found.
// One use case is in TestMain()s to ensure that all tests are cleaned up.
func GetLeaks() error {
	return ensureNoLeaks()
}

func ensureNoLeaks() error {
	if err := ensureNoGoroutines(); err != nil {
		return err
	}
	if err := ensureNoOpenSockets(); err != nil {
		return err
	}
	return nil
}

func ensureNoGoroutines() error {
	var ignored = []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("vitess.io/vitess/go/vt/dbconfigs.init.0.func1"),
		goleak.IgnoreTopFunction("vitess.io/vitess/go/vt/vtgate.resetAggregators"),
		goleak.IgnoreTopFunction("vitess.io/vitess/go/vt/vtgate.processQueryInfo"),
		goleak.IgnoreTopFunction("github.com/patrickmn/go-cache.(*janitor).Run"),
		goleak.IgnoreTopFunction("vitess.io/vitess/go/vt/logutil.(*ThrottledLogger).log.func1"),
		goleak.IgnoreTopFunction("vitess.io/vitess/go/vt/vttablet/tabletserver/throttle.initThrottleTicker.func1.1"),
	}

	var err error
	for i := 0; i < 5; i++ {
		err = goleak.Find(ignored...)
		if err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return err
}

func ensureNoOpenSockets() error {
	cmd := exec.Command("lsof", "-a", "-p", strconv.Itoa(os.Getpid()), "-i", "-P", "-V")
	cmd.Stderr = nil
	lsof, err := cmd.Output()
	if err == nil {
		log.Errorf("found open sockets:\n%s", lsof)
	} else {
		if strings.Contains(string(lsof), "no Internet files located") {
			return nil
		}
		log.Errorf("failed to run `lsof`: %v (%q)", err, lsof)
	}
	return err
}
