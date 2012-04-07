/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

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
	Register()
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

func Register() {
	http.Handle("/debug/snitch", http.HandlerFunc(SnitchHandler))
}

// JsonFunc wraps a func() string to create value that satisfies expvar.Var
// the function should return properly escaped json
type JsonFunc func() string

func (f JsonFunc) String() string { return f() }
