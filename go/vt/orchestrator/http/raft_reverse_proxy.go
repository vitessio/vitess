package http

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	orcraft "vitess.io/vitess/go/vt/orchestrator/raft"

	"github.com/go-martini/martini"

	"vitess.io/vitess/go/vt/orchestrator/config"
)

func raftReverseProxy(w http.ResponseWriter, r *http.Request, c martini.Context) {
	if !orcraft.IsRaftEnabled() {
		// No raft, so no reverse proxy to the leader
		return
	}
	if orcraft.IsLeader() {
		// I am the leader. I will handle the request directly.
		return
	}
	if orcraft.GetLeader() == "" {
		return
	}
	if orcraft.LeaderURI.IsThisLeaderURI() {
		// Although I'm not the leader, the value I see for LeaderURI is my own.
		// I'm probably not up-to-date with my raft transaction log and don't have the latest information.
		// But anyway, obviously not going to redirect to myself.
		// Gonna return: this isn't ideal, because I'm not really the leader. If the user tries to
		// run an operation they'll fail.
		return
	}
	url, err := url.Parse(orcraft.LeaderURI.Get())
	if err != nil {
		log.Errore(err)
		return
	}
	r.Header.Del("Accept-Encoding")
	switch strings.ToLower(config.Config.AuthenticationMethod) {
	case "basic", "multi":
		r.SetBasicAuth(config.Config.HTTPAuthUser, config.Config.HTTPAuthPassword)
	}
	proxy := httputil.NewSingleHostReverseProxy(url)
	proxy.ServeHTTP(w, r)
}
