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

func commandPanic(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	worker, err := NewPanicWorker(wr)
	if err != nil {
		return nil, fmt.Errorf("Could not create Panic worker: %v", err)
	}
	return worker, nil
}

func interactivePanic(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {
	wrk, err := NewPanicWorker(wr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("Could not create Panic worker: %v", err)
	}
	return wrk, nil, nil, nil
}

func init() {
	AddCommand("Debugging", Command{"Panic",
		commandPanic, interactivePanic,
		"<message>",
		"For internal tests only. Will call panic() when executed."})
}
