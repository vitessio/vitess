package utils

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func EnsureNoLeaks(t testing.TB) {
	if t.Failed() {
		return
	}
	ensureNoGoroutines(t)
	ensureNoOpenSockets(t)
}

func ensureNoGoroutines(t testing.TB) {
	var ignored = []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("vitess.io/vitess/go/vt/dbconfigs.init.0.func1"),
		goleak.IgnoreTopFunction("vitess.io/vitess/go/vt/vtgate.resetAggregators"),
		goleak.IgnoreTopFunction("vitess.io/vitess/go/vt/vtgate.processQueryInfo"),
		goleak.IgnoreTopFunction("github.com/patrickmn/go-cache.(*janitor).Run"),
	}

	var err error
	for i := 0; i < 5; i++ {
		err = goleak.Find(ignored...)
		if err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal(err)
}

func ensureNoOpenSockets(t testing.TB) {
	cmd := exec.Command("lsof", "-a", "-p", strconv.Itoa(os.Getpid()), "-i", "-P", "-V")
	cmd.Stderr = nil
	lsof, err := cmd.Output()
	if err == nil {
		t.Errorf("found open sockets:\n%s", lsof)
	} else {
		if strings.Contains(string(lsof), "no Internet files located") {
			return
		}
		t.Errorf("failed to run `lsof`: %v (%q)", err, lsof)
	}
}
