// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"html/template"
	"net/http"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/worker"
	"github.com/youtube/vitess/go/vt/wrangler"
)

const indexHTML = `
<!DOCTYPE html>
<head>
  <title>Worker Action Index</title>
</head>
<body>
  <h1>Worker Action Index</h1>
    <li><a href="/diffs">Diffs</a>: shows a list of all the possible diffs to run.</li>
</body>
`

const diffsHTML = `
<!DOCTYPE html>
<head>
  <title>Worker Diff Action Index</title>
</head>
<body>
  <h1>Worker Diff Action Index</h1>
    <li><a href="/diffs/splitdiff">Split Diff</a>: runs a diff for a shard that uses filtered replication.</li>
</body>
`

const splitDiffHTML = `
<!DOCTYPE html>
<head>
  <title>Split Diff Action</title>
</head>
<body>
  <h1>Split Diff Action</h1>

    {{if .Error}}
      <b>Error:</b> {{.Error}}</br>
    {{else}}
      {{range $i, $si := .Shards}}
        <li><a href="/diffs/splitdiff?keyspace={{$si.Keyspace}}&shard={{$si.Shard}}">{{$si.Keyspace}}/{{$si.Shard}}</a></li>
      {{end}}
    {{end}}
</body>
`

func httpError(w http.ResponseWriter, format string, err error) {
	log.Errorf(format, err)
	http.Error(w, fmt.Sprintf(format, err), http.StatusInternalServerError)
}

func loadTemplate(name, contents string) *template.Template {
	t, err := template.New(name).Parse(contents)
	if err != nil {
		log.Fatalf("Cannot parse %v template: %v", name, err)
	}
	return t
}

func executeTemplate(w http.ResponseWriter, t *template.Template, data interface{}) {
	if err := t.Execute(w, data); err != nil {
		httpError(w, "error executing template", err)
	}
}

// shardsWithSources returns all the shards that have SourceShards set.
func shardsWithSources(wr *wrangler.Wrangler) ([]map[string]string, error) {
	keyspaces, err := wr.TopoServer().GetKeyspaces()
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	mu := sync.Mutex{} // protects result
	result := make([]map[string]string, 0, len(keyspaces))
	rec := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			shards, err := wr.TopoServer().GetShardNames(keyspace)
			if err != nil {
				rec.RecordError(err)
				return
			}
			for _, shard := range shards {
				wg.Add(1)
				go func(keyspace, shard string) {
					defer wg.Done()
					si, err := wr.TopoServer().GetShard(keyspace, shard)
					if err != nil {
						rec.RecordError(err)
						return
					}

					if len(si.SourceShards) > 0 {
						mu.Lock()
						result = append(result, map[string]string{
							"Keyspace": keyspace,
							"Shard":    shard,
						})
						mu.Unlock()
					}
				}(keyspace, shard)
			}
		}(keyspace)
	}
	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("There are no shards with SourceShards")
	}
	return result, nil
}

func initInteractiveMode(wr *wrangler.Wrangler) {
	indexTemplate := loadTemplate("index", indexHTML)
	diffsTemplate := loadTemplate("diffs", diffsHTML)
	splitDiffTemplate := loadTemplate("splitdiff", splitDiffHTML)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		executeTemplate(w, indexTemplate, nil)
	})
	http.HandleFunc("/diffs", func(w http.ResponseWriter, r *http.Request) {
		executeTemplate(w, diffsTemplate, nil)
	})
	http.HandleFunc("/diffs/splitdiff", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			httpError(w, "cannot parse form: %s", err)
			return
		}
		keyspace := r.FormValue("keyspace")
		shard := r.FormValue("shard")

		if keyspace == "" || shard == "" {
			result := make(map[string]interface{})

			shards, err := shardsWithSources(wr)
			if err != nil {
				result["Error"] = err.Error()
			} else {
				result["Shards"] = shards
			}

			executeTemplate(w, splitDiffTemplate, result)
		} else {
			wrk := worker.NewSplitDiffWorker(wr, keyspace, shard)
			if _, err := setAndStartWorker(wrk); err != nil {
				httpError(w, "cannot set worker: %s", err)
				return
			}

			http.Redirect(w, r, "/status", http.StatusTemporaryRedirect)
		}
	})
	log.Infof("Interactive mode ready")
}
