/*
Copyright 2023 The Vitess Authors.

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

package opentsdb

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
)

var singletonBackend stats.PushBackend

// Init attempts to create a singleton *opentsdb.backend and register it as a PushBackend.
// If it fails to create one, this is a noop. The prefix argument is an optional string
// to prepend to the name of every data point reported.
func Init(prefix string) {
	// Needs to happen in servenv.OnRun() instead of init because it requires flag parsing and logging
	servenv.OnRun(func() {
		log.Info("Initializing opentsdb backend...")
		backend, err := InitWithoutServenv(prefix)
		if err != nil {
			log.Infof("Failed to initialize singleton opentsdb backend: %v", err)
		} else {
			singletonBackend = backend
			log.Info("Initialized opentsdb backend.")
		}
	})
}

// InitWithoutServenv initializes the opentsdb without servenv
func InitWithoutServenv(prefix string) (stats.PushBackend, error) {
	b, err := newBackend(prefix)

	if err != nil {
		return nil, err
	}

	stats.RegisterPushBackend("opentsdb", b)

	servenv.HTTPHandleFunc("/debug/opentsdb", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		collector := b.collector()
		collector.collectAll()
		data := collector.data
		sort.Sort(byMetric(data))

		if b, err := json.MarshalIndent(data, "", "  "); err != nil {
			w.Write([]byte(err.Error()))
		} else {
			w.Write(b)
		}
	})

	return b, nil
}

func newBackend(prefix string) (*backend, error) {
	if openTSDBURI == "" {
		return nil, fmt.Errorf("cannot create opentsdb PushBackend with empty --opentsdb_uri")
	}

	var w writer

	// Use the file API when the uri is in format file://...
	u, err := url.Parse(openTSDBURI)
	if err != nil {
		return nil, fmt.Errorf("failed to parse --opentsdb_uri %s: %v", openTSDBURI, err)
	} else if u.Scheme == "file" {
		fw, err := newFileWriter(u.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to create file-based writer for --opentsdb_uri %s: %v", openTSDBURI, err)
		} else {
			w = fw
		}
	} else {
		w = newHTTPWriter(&http.Client{}, openTSDBURI)
	}

	return &backend{
		prefix:     prefix,
		commonTags: stats.ParseCommonTags(stats.CommonTags),
		writer:     w,
	}, nil
}
