// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package umgmt

import (
	"testing"

	log "github.com/golang/glog"
)

var ready = make(chan bool)

func serve(t *testing.T) {
	AddStartupCallback(func() { ready <- true })
	AddShutdownCallback(func() { log.Infof("test server GracefulShutdown callback") })
	err := ListenAndServe("/tmp/test-sock")
	if err != nil {
		t.Fatalf("listen err: %v", err)
	}
	log.Infof("test server finished")
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
	log.Infof("Ping reply: %v", reply.Message)

	reply = new(Reply)
	callErr = client.Call("UmgmtService.CloseListeners", reply, reply)
	if callErr != nil {
		t.Fatalf("callErr: %v", callErr)
	}
	log.Infof("CloseListeners reply: %v", reply.Message)

	reply = new(Reply)
	callErr = client.Call("UmgmtService.GracefulShutdown", reply, reply)
	if callErr != nil {
		t.Fatalf("callErr: %v", callErr)
	}
	log.Infof("GracefulShutdown reply: %v", reply.Message)
}
