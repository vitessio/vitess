// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package umgmt

import (
	"testing"

	"code.google.com/p/vitess/go/relog"
)

var ready = make(chan bool)

func serve(t *testing.T) {
	AddStartupCallback(func() { ready <- true })
	AddShutdownCallback(func() { relog.Info("test server GracefulShutdown callback") })
	err := ListenAndServe("/tmp/test-sock")
	if err != nil {
		t.Fatalf("listen err: %v", err)
	}
	relog.Info("test server finished")
}

func TestUmgmt(t *testing.T) {
	go serve(t)
	<-ready

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
	relog.Info("Ping reply: %v", reply.Message)

	reply = new(Reply)
	callErr = client.Call("UmgmtService.CloseListeners", reply, reply)
	if callErr != nil {
		t.Fatalf("callErr: %v", callErr)
	}
	relog.Info("CloseListeners reply: %v", reply.Message)

	reply = new(Reply)
	callErr = client.Call("UmgmtService.GracefulShutdown", reply, reply)
	if callErr != nil {
		t.Fatalf("callErr: %v", callErr)
	}
	relog.Info("GracefulShutdown reply: %v", reply.Message)
}
