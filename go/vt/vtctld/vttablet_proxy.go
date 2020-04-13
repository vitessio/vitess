/*
Copyright 2020 The Vitess Authors.

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

package vtctld

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"

	"vitess.io/vitess/go/vt/log"
)

var (
	proxyTablets = flag.Bool("proxy_tablets", false, "Setting this true will make vtctld proxy the tablet status instead of redirecting to them")

	remotesMu sync.Mutex
	remotes   = make(map[string]*redirection)
)

type redirection struct {
	mu         sync.Mutex
	url        *url.URL
	prefixPath string
}

func newRedirection(u *url.URL, prefixPath string) *redirection {
	rd := &redirection{
		url:        u,
		prefixPath: prefixPath,
	}

	// rp is created once per prefixPath. But the data it uses
	// to redirect is controlled by redirection.
	rp := &httputil.ReverseProxy{}
	rp.Director = func(req *http.Request) {
		rd.mu.Lock()
		defer rd.mu.Unlock()

		splits := strings.SplitN(req.URL.Path, "/", 4)
		if len(splits) < 4 {
			return
		}
		req.URL.Scheme = rd.url.Scheme
		req.URL.Host = rd.url.Host
		req.URL.Path = "/" + splits[3]
	}

	rp.ModifyResponse = func(r *http.Response) error {
		rd.mu.Lock()
		defer rd.mu.Unlock()

		b, _ := ioutil.ReadAll(r.Body)
		b = bytes.ReplaceAll(b, []byte(`href="/`), []byte(fmt.Sprintf(`href="%s`, rd.prefixPath)))
		b = bytes.ReplaceAll(b, []byte(`href=/`), []byte(fmt.Sprintf(`href=%s`, rd.prefixPath)))
		r.Body = ioutil.NopCloser(bytes.NewBuffer(b))
		r.Header["Content-Length"] = []string{fmt.Sprint(len(b))}
		// Don't forget redirects
		loc := r.Header["Location"]
		for i, v := range loc {
			loc[i] = strings.Replace(v, "/", rd.prefixPath, 1)
		}
		return nil
	}
	http.Handle(prefixPath, rp)
	return rd
}

func (rd *redirection) set(u *url.URL, prefixPath string) {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	rd.url = u
	rd.prefixPath = prefixPath
}

func addRemote(tabletID, path string) {
	u, err := url.Parse(path)
	if err != nil {
		log.Errorf("Error parsing URL %v: %v", path, err)
		return
	}
	prefixPath := fmt.Sprintf("/vttablet/%s/", tabletID)

	remotesMu.Lock()
	defer remotesMu.Unlock()
	if rd, ok := remotes[tabletID]; ok {
		rd.set(u, prefixPath)
		return
	}
	remotes[tabletID] = newRedirection(u, prefixPath)
}
