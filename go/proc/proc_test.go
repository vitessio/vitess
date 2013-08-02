// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proc

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

var port = flag.String("port", "", "http port")

func TestRestart(t *testing.T) {
	switch os.Getenv("SERVER_NUM") {
	case "":
		testLaunch(t)
	case "1":
		testServer(t, syscall.SIGUSR1)
	case "2":
		testServer(t, syscall.SIGTERM)
	}
}

var testPort = "12345"

func testLaunch(t *testing.T) {
	var err error

	cmd1 := launchServer(t, 1)
	defer cmd1.Process.Kill()
	testPid(t, cmd1.Process.Pid)

	cmd2 := launchServer(t, 2)
	defer cmd2.Process.Kill()
	err = cmd1.Wait()
	if err != nil {
		t.Error(err)
	}
	testPid(t, cmd2.Process.Pid)

	err = syscall.Kill(cmd2.Process.Pid, syscall.SIGTERM)
	if err != nil {
		t.Error(err)
	}
	err = cmd2.Wait()
	if err != nil {
		t.Error(err)
	}
}

func launchServer(t *testing.T, num int) *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run=^TestRestart$", "-port", testPort)
	cmd.Env = []string{fmt.Sprintf("SERVER_NUM=%d", num)}
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		t.Fatal(err)
	}
	return cmd
}

func testPid(t *testing.T, want int) {
	var resp *http.Response
	var err error
	for i := 0; i < 20; i++ {
		resp, err = http.Get(fmt.Sprintf("http://localhost:%s%s", testPort, pidURL))
		if err != nil {
			if i == 19 {
				t.Fatal(err)
			}
			if strings.Contains(err.Error(), "connection refused") {
				time.Sleep(1000 * time.Millisecond)
				continue
			}
			t.Fatalf("unexpected error on port %v: %v", testPort, err)
		}
		break
	}
	num, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("could not read pid: %vd", err)
	}
	got, err := strconv.Atoi(string(num))
	if err != nil {
		t.Fatalf("could not read pid: %vd", err)
	}
	if want != got {
		t.Errorf("want %d, got %d", want, got)
	}
}

func testServer(t *testing.T, want syscall.Signal) {
	flag.Parse()
	if *port != "12345" {
		t.Errorf("want 12345, got %s", *port)
	}

	l, err := Listen(*port)
	if err != nil {
		t.Fatalf("could not initialize listener: %v", err)
	}
	go http.Serve(l, nil)
	got := Wait()
	l.Close()
	if want != got {
		t.Errorf("want %v, got %v", want, got)
	}
}
