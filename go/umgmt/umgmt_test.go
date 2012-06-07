// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package umgmt

import (
	"code.google.com/p/vitess/go/relog"
	"testing"
	"time"
)

func serve() {
	AddShutdownCallback(ShutdownCallback(func() error { relog.Error("testserver GracefulShutdown callback"); return nil }))
	err := ListenAndServe("/tmp/test-sock")
	if err != nil {
		relog.Fatal("listen err:%v", err)
	}
	relog.Info("test server finished")
}

func TestUmgmt(t *testing.T) {
	go serve()
	time.Sleep(1e9)

	client, err := Dial("/tmp/test-sock")
	if err != nil {
		t.Fatalf("can't connect %v", err)
	}
	request := new(Request)
	reply := new(Reply)

	callErr := client.Call("UmgmtService.Ping", request, reply)
	if callErr != nil {
		t.Fatalf("callErr: %v", callErr)
	}
	relog.Info("reply: %v", reply.Message)

	callErr = client.Call("UmgmtService.CloseListeners", reply, reply)
	if callErr != nil {
		t.Fatalf("callErr: %v", callErr)
	}
	relog.Info("reply: %v", reply.Message)
	time.Sleep(5e9)
	callErr = client.Call("UmgmtService.GracefulShutdown", reply, reply)
	if callErr != nil {
		t.Fatalf("callErr: %v", callErr)
	}
	relog.Info("reply: %v", reply.Message)
}
