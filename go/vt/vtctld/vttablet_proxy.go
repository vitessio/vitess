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

	proxyMu sync.Mutex
	remotes = make(map[string]*httputil.ReverseProxy)
)

func addRemote(tabletID, path string) {
	u, err := url.Parse(path)
	if err != nil {
		log.Errorf("Error parsing URL %v: %v", path, err)
		return
	}

	proxyMu.Lock()
	defer proxyMu.Unlock()
	if _, ok := remotes[tabletID]; ok {
		return
	}

	rp := &httputil.ReverseProxy{}
	rp.Director = func(req *http.Request) {
		splits := strings.SplitN(req.URL.Path, "/", 4)
		if len(splits) < 4 {
			return
		}
		req.URL.Scheme = u.Scheme
		req.URL.Host = u.Host
		req.URL.Path = "/" + splits[3]
	}

	prefixPath := fmt.Sprintf("/vttablet/%s/", tabletID)

	rp.ModifyResponse = func(r *http.Response) error {
		b, _ := ioutil.ReadAll(r.Body)
		b = bytes.ReplaceAll(b, []byte(`href="/`), []byte(fmt.Sprintf(`href="%s`, prefixPath)))
		b = bytes.ReplaceAll(b, []byte(`href=/`), []byte(fmt.Sprintf(`href=%s`, prefixPath)))
		r.Body = ioutil.NopCloser(bytes.NewBuffer(b))
		r.Header["Content-Length"] = []string{fmt.Sprint(len(b))}
		// Don't forget redirects
		loc := r.Header["Location"]
		for i, v := range loc {
			loc[i] = strings.Replace(v, "/", prefixPath, 1)
		}
		return nil
	}
	http.Handle(prefixPath, rp)
	remotes[tabletID] = rp
}
