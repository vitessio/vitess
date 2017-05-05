/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package worker

import (
	"flag"
	"fmt"
	"html/template"
	"net/http"

	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"
)

const blockHTML = `
<!DOCTYPE html>
<head>
  <title>Block Action</title>
</head>
<body>
  <h1>Block Action</h1>

    {{if .Error}}
      <b>Error:</b> {{.Error}}</br>
    {{else}}
	    <form action="/Debugging/Block" method="post">
	      <INPUT type="submit" name="submit" value="Block (until canceled)"/>
    </form>
    {{end}}
</body>
`

var blockTemplate = mustParseTemplate("block", blockHTML)

func commandBlock(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	if err := subFlags.Parse(args); err != nil {
		return nil, err
	}
	if subFlags.NArg() != 0 {
		subFlags.Usage()
		return nil, fmt.Errorf("command Block does not accept any parameter")
	}

	worker, err := NewBlockWorker(wr)
	if err != nil {
		return nil, fmt.Errorf("Could not create Block worker: %v", err)
	}
	return worker, nil
}

func interactiveBlock(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, nil, fmt.Errorf("Cannot parse form: %s", err)
	}

	if submit := r.FormValue("submit"); submit == "" {
		result := make(map[string]interface{})
		return nil, blockTemplate, result, nil
	}

	wrk, err := NewBlockWorker(wr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("Could not create Block worker: %v", err)
	}
	return wrk, nil, nil, nil
}

func init() {
	AddCommand("Debugging", Command{"Block",
		commandBlock, interactiveBlock,
		"<message>",
		"For internal tests only. When triggered, the command will block until canceled."})
}
