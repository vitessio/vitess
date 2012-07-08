// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package umgmt

import (
	"testing"
)

var ready = make(chan bool)

func serve(t *testing.T) {
	AddStartupCallback(func() { ready <- true })
	AddShutdownCallback(func() { t.Log("test server GracefulShutdown callback") })
	err := ListenAndServe("/tmp/test-sock")
	if err != nil {
		t.Fatalf("listen err: %v", err)
	}
	t.Log("test server finished")
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
	t.Logf("Ping reply: %v", reply.Message)

	reply = new(Reply)
	callErr = client.Call("UmgmtService.CloseListeners", reply, reply)
	if callErr != nil {
		t.Fatalf("callErr: %v", callErr)
	}
	t.Logf("CloseListeners reply: %v", reply.Message)


	reply = new(Reply)
	callErr = client.Call("UmgmtService.GracefulShutdown", reply, reply)
	if callErr != nil {
		t.Fatalf("callErr: %v", callErr)
	}
	t.Logf("GracefulShutdown reply: %v", reply.Message)
}
