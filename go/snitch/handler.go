// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package snitch

import (
	"code.google.com/p/vitess/go/relog"
	"fmt"
	"net/http"
	"runtime"
)

const (
	BaseUrl = "/debug/snitch"
)

type SnitchCmd struct {
	url         string
	description string
	handler     http.HandlerFunc
}

var cmdList []SnitchCmd

func init() {
	cmdList = []SnitchCmd{
		{"gc", "Force garbage collection", GcHandler},
		{"panic", "Force panic (will crash app)", PanicHandler},
	}
	http.Handle("/debug/snitch", http.HandlerFunc(SnitchHandler))
}

func SnitchHandler(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(response, "<form method=\"get\"><br>snitch server<br>\n")
	for _, cmd := range cmdList {
		if request.FormValue(cmd.url) != "" {
			cmd.handler(response, request)
		}
		fmt.Fprintf(response, "<p><input type=\"submit\" name=\"%s\" value=\"%s\"></input></p>\n", cmd.url, cmd.description)
	}
	fmt.Fprintf(response, "</form>\n")
}

func GcHandler(response http.ResponseWriter, request *http.Request) {
	go func() {
		// NOTE(msolomon) I'm not sure if this blocks or not - a cursory glance at the
		// code didn't reveal enough and I'm being lazy
		relog.Info("start forced garbage collection")
		runtime.GC()
		relog.Info("finished forced garbage collection")
	}()

	data := "forced gc\n"
	response.Write([]byte(data))
}

func PanicHandler(response http.ResponseWriter, request *http.Request) {
	// Make the panic happen in a goroutine. Otherwise, http framework traps it.
	go func() {
		panic("intentional")
	}()
}

func RegisterCommand(path, description string, handler http.HandlerFunc) {
	cmdList = append(cmdList, SnitchCmd{path, description, handler})
}

// JsonFunc wraps a func() string to create value that satisfies expvar.Var
// the function should return properly escaped json
type JsonFunc func() string

func (f JsonFunc) String() string { return f() }
