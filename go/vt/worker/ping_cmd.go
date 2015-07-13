// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"flag"
	"fmt"
	"html/template"
	"net/http"

	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"
)

const pingHTML = `
<!DOCTYPE html>
<head>
  <title>Ping Action</title>
</head>
<body>
  <h1>Ping Action</h1>

    {{if .Error}}
      <b>Error:</b> {{.Error}}</br>
    {{else}}
	    <form action="/Debugging/Ping" method="post">
	      <LABEL for="message">Message to be logged with level CONSOLE: </LABEL>
					<INPUT type="text" id="message" name="message" value="pong"></BR>
	      <INPUT type="submit" value="Ping"/>
    </form>
    {{end}}
</body>
`

var pingTemplate = mustParseTemplate("ping", pingHTML)

func commandPing(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	if err := subFlags.Parse(args); err != nil {
		return nil, err
	}
	if subFlags.NArg() != 1 {
		subFlags.Usage()
		return nil, fmt.Errorf("command Ping requires <message>")
	}
	message := subFlags.Arg(0)

	worker, err := NewPingWorker(wr, message)
	if err != nil {
		return nil, fmt.Errorf("Could not create Ping worker: %v", err)
	}
	return worker, nil
}

func interactivePing(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, nil, fmt.Errorf("Cannot parse form: %s", err)
	}

	message := r.FormValue("message")
	if message == "" {
		result := make(map[string]interface{})
		return nil, pingTemplate, result, nil
	}

	wrk, err := NewPingWorker(wr, message)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("Could not create Ping worker: %v", err)
	}
	return wrk, nil, nil, nil
}

func init() {
	AddCommand("Debugging", Command{"Ping",
		commandPing, interactivePing,
		"<message>",
		"For internal tests only. <message> will be logged as CONSOLE output."})
}
