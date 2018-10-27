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
	"html/template"
	"net/http"

	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/wrangler"
)

func commandPanic(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	worker, err := NewPanicWorker(wr)
	if err != nil {
		return nil, vterrors.Wrap(err, "Could not create Panic worker")
	}
	return worker, nil
}

func interactivePanic(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {
	wrk, err := NewPanicWorker(wr)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "Could not create Panic worker")
	}
	return wrk, nil, nil, nil
}

func init() {
	AddCommand("Debugging", Command{"Panic",
		commandPanic, interactivePanic,
		"<message>",
		"For internal tests only. Will call panic() when executed."})
}
