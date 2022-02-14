package vtctld

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

func initVTTabletRedirection(ts *topo.Server) {
	http.HandleFunc("/vttablet/", func(w http.ResponseWriter, r *http.Request) {
		splits := strings.SplitN(r.URL.Path, "/", 4)
		if len(splits) < 4 {
			log.Errorf("Invalid URL: %v", r.URL)
			http.NotFound(w, r)
			return
		}
		tabletID := splits[2]
		tabletAlias, err := topoproto.ParseTabletAlias(tabletID)
		if err != nil {
			log.Errorf("Error parsting tablet alias %v: %v", tabletID, err)
			http.NotFound(w, r)
			return
		}
		tablet, err := ts.GetTablet(r.Context(), tabletAlias)
		if err != nil {
			log.Errorf("Error fetching tablet %v: %v", splits[2], err)
			http.NotFound(w, r)
			return
		}
		if tablet.Hostname == "" || tablet.PortMap["vt"] == 0 {
			log.Errorf("Invalid host/port: %s %d", tablet.Hostname, tablet.PortMap["vt"])
			http.NotFound(w, r)
			return
		}

		rp := &httputil.ReverseProxy{}
		rp.Director = func(req *http.Request) {
			req.URL.Scheme = "http"
			req.URL.Host = netutil.JoinHostPort(tablet.Hostname, tablet.PortMap["vt"])
			req.URL.Path = "/" + splits[3]
		}

		prefixPath := fmt.Sprintf("/vttablet/%s/", tabletID)
		rp.ModifyResponse = func(r *http.Response) error {
			b, _ := io.ReadAll(r.Body)
			b = bytes.ReplaceAll(b, []byte(`href="/`), []byte(fmt.Sprintf(`href="%s`, prefixPath)))
			b = bytes.ReplaceAll(b, []byte(`href=/`), []byte(fmt.Sprintf(`href=%s`, prefixPath)))
			r.Body = io.NopCloser(bytes.NewBuffer(b))
			r.Header["Content-Length"] = []string{strconv.FormatInt(int64(len(b)), 10)}

			// Don't forget redirects
			loc := r.Header["Location"]
			for i, v := range loc {
				if strings.HasPrefix(v, "/") {
					loc[i] = strings.Replace(v, "/", prefixPath, 1)
				}
			}
			return nil
		}

		rp.ServeHTTP(w, r)
	})
}
