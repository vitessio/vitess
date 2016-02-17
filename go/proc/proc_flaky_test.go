// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proc

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode.")
	}

	switch os.Getenv("SERVER_NUM") {
	case "":
		testLaunch(t)
	case "1":
		testServer(t, syscall.SIGUSR1)
	case "2":
		testServer(t, syscall.SIGTERM)
	}
}

func testLaunch(t *testing.T) {
	var err error
	l, err := net.Listen("tcp", "")
	if err != nil {
		t.Fatalf("could not initialize listener: %v", err)
	}
	hostport := l.Addr().String()
	l.Close()
	_, port, err := net.SplitHostPort(hostport)
	if err != nil {
		t.Fatal(err)
	}

	cmd1 := launchServer(t, port, 1)
	defer cmd1.Process.Kill()
	testPid(t, port, cmd1.Process.Pid)

	cmd2 := launchServer(t, port, 2)
	defer cmd2.Process.Kill()
	err = cmd1.Wait()
	if err != nil {
		t.Error(err)
	}
	testPid(t, port, cmd2.Process.Pid)

	err = syscall.Kill(cmd2.Process.Pid, syscall.SIGTERM)
	if err != nil {
		t.Error(err)
	}
	err = cmd2.Wait()
	if err != nil {
		t.Error(err)
	}
}

func launchServer(t *testing.T, port string, num int) *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run=^TestRestart$")
	cmd.Env = []string{
		fmt.Sprintf("SERVER_NUM=%d", num),
		fmt.Sprintf("PORT=%s", port),
	}
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		t.Fatal(err)
	}
	return cmd
}

func testPid(t *testing.T, port string, want int) {
	var resp *http.Response
	var err error
	retryIntervalMs := 50
	// Retry up to two seconds.
	for i := 0; i < (2000 / retryIntervalMs); i++ {
		resp, err = http.Get(fmt.Sprintf("http://localhost:%s%s", port, pidURL))
		var retryableErr bool
		if err != nil {
			if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "EOF") ||
				strings.Contains(err.Error(), "net/http: transport closed before response was received") ||
				strings.Contains(err.Error(), "http: can't write HTTP request on broken connection") {
				retryableErr = true
			}
		}
		if err == nil && resp.StatusCode != http.StatusOK {
			err = fmt.Errorf("http request was not successful. status: %v", resp.Status)
			if resp.StatusCode == http.StatusNotFound {
				// Observable when the handler is not installed yet.
				retryableErr = true
			}
		}
		if err != nil && retryableErr {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		break
	}
	if err != nil {
		t.Fatalf("unexpected error on port %v: %v", port, err)
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
	l, err := Listen(os.Getenv("PORT"))
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
