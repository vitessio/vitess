/*
   Copyright 2014 Outbrain Inc.

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

package http

import (
	"expvar"
	"fmt"
	"net/http"
	"net/http/pprof"
	"strconv"
	"text/template"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/auth"
	"github.com/martini-contrib/render"
	"github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/inst"
)

// HttpWeb is the web requests server, mapping each request to a web page
type HttpWeb struct {
	URLPrefix string
}

var Web HttpWeb = HttpWeb{}

func (this *HttpWeb) getInstanceKey(host string, port string) (inst.InstanceKey, error) {
	instanceKey := inst.InstanceKey{Hostname: host}
	var err error

	if instanceKey.Port, err = strconv.Atoi(port); err != nil {
		return instanceKey, fmt.Errorf("Invalid port: %s", port)
	}
	return instanceKey, err
}

func (this *HttpWeb) AccessToken(params martini.Params, r render.Render, req *http.Request, resp http.ResponseWriter, user auth.User) {
	publicToken := template.JSEscapeString(req.URL.Query().Get("publicToken"))
	err := authenticateToken(publicToken, resp)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	r.Redirect(this.URLPrefix + "/")
}

func (this *HttpWeb) Index(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	// Redirect index so that all web URLs begin with "/web/".
	// We also redirect /web/ to /web/clusters so that
	// the Clusters page has a single canonical URL.
	r.Redirect(this.URLPrefix + "/web/clusters")
}

func (this *HttpWeb) Clusters(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/clusters", map[string]interface{}{
		"agentsHttpActive":              config.Config.ServeAgentsHttp,
		"title":                         "clusters",
		"autoshow_problems":             false,
		"authorizedForAction":           isAuthorizedForAction(req, user),
		"userId":                        getUserId(req, user),
		"removeTextFromHostnameDisplay": config.Config.RemoveTextFromHostnameDisplay,
		"prefix":                        this.URLPrefix,
		"webMessage":                    config.Config.WebMessage,
	})
}

func (this *HttpWeb) ClustersAnalysis(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/clusters_analysis", map[string]interface{}{
		"agentsHttpActive":              config.Config.ServeAgentsHttp,
		"title":                         "clusters",
		"autoshow_problems":             false,
		"authorizedForAction":           isAuthorizedForAction(req, user),
		"userId":                        getUserId(req, user),
		"removeTextFromHostnameDisplay": config.Config.RemoveTextFromHostnameDisplay,
		"prefix":                        this.URLPrefix,
		"webMessage":                    config.Config.WebMessage,
	})
}

func (this *HttpWeb) Cluster(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	clusterName, _ := figureClusterName(params["clusterName"])

	r.HTML(200, "templates/cluster", map[string]interface{}{
		"agentsHttpActive":              config.Config.ServeAgentsHttp,
		"title":                         "cluster",
		"clusterName":                   clusterName,
		"autoshow_problems":             true,
		"contextMenuVisible":            true,
		"pseudoGTIDModeEnabled":         (config.Config.PseudoGTIDPattern != ""),
		"authorizedForAction":           isAuthorizedForAction(req, user),
		"userId":                        getUserId(req, user),
		"removeTextFromHostnameDisplay": config.Config.RemoveTextFromHostnameDisplay,
		"compactDisplay":                template.JSEscapeString(req.URL.Query().Get("compact")),
		"prefix":                        this.URLPrefix,
		"webMessage":                    config.Config.WebMessage,
	})
}

func (this *HttpWeb) ClusterByAlias(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	clusterName, err := inst.GetClusterByAlias(params["clusterAlias"])
	// Willing to accept the case of multiple clusters; we just present one
	if clusterName == "" && err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	params["clusterName"] = clusterName
	this.Cluster(params, r, req, user)
}

func (this *HttpWeb) ClusterByInstance(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	instanceKey, err := this.getInstanceKey(params["host"], params["port"])
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, found, err := inst.ReadInstance(&instanceKey)
	if (!found) || (err != nil) {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", instanceKey)})
		return
	}

	// Willing to accept the case of multiple clusters; we just present one
	if instance.ClusterName == "" && err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	params["clusterName"] = instance.ClusterName
	this.Cluster(params, r, req, user)
}

func (this *HttpWeb) ClusterPools(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	clusterName, _ := figureClusterName(params["clusterName"])
	r.HTML(200, "templates/cluster_pools", map[string]interface{}{
		"agentsHttpActive":              config.Config.ServeAgentsHttp,
		"title":                         "cluster pools",
		"clusterName":                   clusterName,
		"autoshow_problems":             false, // because pool screen by default expands all hosts
		"contextMenuVisible":            true,
		"pseudoGTIDModeEnabled":         (config.Config.PseudoGTIDPattern != ""),
		"authorizedForAction":           isAuthorizedForAction(req, user),
		"userId":                        getUserId(req, user),
		"removeTextFromHostnameDisplay": config.Config.RemoveTextFromHostnameDisplay,
		"compactDisplay":                template.JSEscapeString(req.URL.Query().Get("compact")),
		"prefix":                        this.URLPrefix,
		"webMessage":                    config.Config.WebMessage,
	})
}

func (this *HttpWeb) Search(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	searchString := params["searchString"]
	if searchString == "" {
		searchString = req.URL.Query().Get("s")
	}
	searchString = template.JSEscapeString(searchString)
	r.HTML(200, "templates/search", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "search",
		"searchString":        searchString,
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"prefix":              this.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (this *HttpWeb) Discover(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/discover", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "discover",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"prefix":              this.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (this *HttpWeb) Audit(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	page, err := strconv.Atoi(params["page"])
	if err != nil {
		page = 0
	}

	r.HTML(200, "templates/audit", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "audit",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"page":                page,
		"auditHostname":       params["host"],
		"auditPort":           params["port"],
		"prefix":              this.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (this *HttpWeb) AuditRecovery(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	page, err := strconv.Atoi(params["page"])
	if err != nil {
		page = 0
	}
	recoveryId, err := strconv.ParseInt(params["id"], 10, 0)
	if err != nil {
		recoveryId = 0
	}
	recoveryUid := params["uid"]
	clusterAlias := params["clusterAlias"]

	clusterName, _ := figureClusterName(params["clusterName"])
	r.HTML(200, "templates/audit_recovery", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "audit-recovery",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"page":                page,
		"clusterName":         clusterName,
		"clusterAlias":        clusterAlias,
		"recoveryId":          recoveryId,
		"recoveryUid":         recoveryUid,
		"prefix":              this.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (this *HttpWeb) AuditFailureDetection(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	page, err := strconv.Atoi(params["page"])
	if err != nil {
		page = 0
	}
	detectionId, err := strconv.ParseInt(params["id"], 10, 0)
	if err != nil {
		detectionId = 0
	}
	clusterAlias := params["clusterAlias"]

	r.HTML(200, "templates/audit_failure_detection", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "audit-failure-detection",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"page":                page,
		"detectionId":         detectionId,
		"clusterAlias":        clusterAlias,
		"prefix":              this.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (this *HttpWeb) Agents(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/agents", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "agents",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"prefix":              this.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (this *HttpWeb) Agent(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/agent", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "agent",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"agentHost":           params["host"],
		"prefix":              this.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (this *HttpWeb) AgentSeedDetails(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/agent_seed_details", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "agent seed details",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"seedId":              params["seedId"],
		"prefix":              this.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (this *HttpWeb) Seeds(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/seeds", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "seeds",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"prefix":              this.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (this *HttpWeb) Home(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/home", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "home",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"prefix":              this.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (this *HttpWeb) About(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/about", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "about",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"prefix":              this.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (this *HttpWeb) KeepCalm(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/keep-calm", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "Keep Calm",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"prefix":              this.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (this *HttpWeb) FAQ(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/faq", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "FAQ",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"prefix":              this.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (this *HttpWeb) Status(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/status", map[string]interface{}{
		"agentsHttpActive":    config.Config.ServeAgentsHttp,
		"title":               "status",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserId(req, user),
		"autoshow_problems":   false,
		"prefix":              this.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (this *HttpWeb) registerWebRequest(m *martini.ClassicMartini, path string, handler martini.Handler) {
	fullPath := fmt.Sprintf("%s/web/%s", this.URLPrefix, path)
	if path == "/" {
		fullPath = fmt.Sprintf("%s/", this.URLPrefix)
	}

	if config.Config.RaftEnabled {
		m.Get(fullPath, raftReverseProxy, handler)
	} else {
		m.Get(fullPath, handler)
	}
}

// RegisterRequests makes for the de-facto list of known Web calls
func (this *HttpWeb) RegisterRequests(m *martini.ClassicMartini) {
	this.registerWebRequest(m, "access-token", this.AccessToken)
	this.registerWebRequest(m, "", this.Index)
	this.registerWebRequest(m, "/", this.Index)
	this.registerWebRequest(m, "home", this.About)
	this.registerWebRequest(m, "about", this.About)
	this.registerWebRequest(m, "keep-calm", this.KeepCalm)
	this.registerWebRequest(m, "faq", this.FAQ)
	this.registerWebRequest(m, "status", this.Status)
	this.registerWebRequest(m, "clusters", this.Clusters)
	this.registerWebRequest(m, "clusters-analysis", this.ClustersAnalysis)
	this.registerWebRequest(m, "cluster/:clusterName", this.Cluster)
	this.registerWebRequest(m, "cluster/alias/:clusterAlias", this.ClusterByAlias)
	this.registerWebRequest(m, "cluster/instance/:host/:port", this.ClusterByInstance)
	this.registerWebRequest(m, "cluster-pools/:clusterName", this.ClusterPools)
	this.registerWebRequest(m, "search/:searchString", this.Search)
	this.registerWebRequest(m, "search", this.Search)
	this.registerWebRequest(m, "discover", this.Discover)
	this.registerWebRequest(m, "audit", this.Audit)
	this.registerWebRequest(m, "audit/:page", this.Audit)
	this.registerWebRequest(m, "audit/instance/:host/:port", this.Audit)
	this.registerWebRequest(m, "audit/instance/:host/:port/:page", this.Audit)
	this.registerWebRequest(m, "audit-recovery", this.AuditRecovery)
	this.registerWebRequest(m, "audit-recovery/:page", this.AuditRecovery)
	this.registerWebRequest(m, "audit-recovery/id/:id", this.AuditRecovery)
	this.registerWebRequest(m, "audit-recovery/uid/:uid", this.AuditRecovery)
	this.registerWebRequest(m, "audit-recovery/cluster/:clusterName", this.AuditRecovery)
	this.registerWebRequest(m, "audit-recovery/cluster/:clusterName/:page", this.AuditRecovery)
	this.registerWebRequest(m, "audit-recovery/alias/:clusterAlias", this.AuditRecovery)
	this.registerWebRequest(m, "audit-recovery/alias/:clusterAlias/:page", this.AuditRecovery)
	this.registerWebRequest(m, "audit-failure-detection", this.AuditFailureDetection)
	this.registerWebRequest(m, "audit-failure-detection/:page", this.AuditFailureDetection)
	this.registerWebRequest(m, "audit-failure-detection/id/:id", this.AuditFailureDetection)
	this.registerWebRequest(m, "audit-failure-detection/alias/:clusterAlias", this.AuditFailureDetection)
	this.registerWebRequest(m, "audit-failure-detection/alias/:clusterAlias/:page", this.AuditFailureDetection)
	this.registerWebRequest(m, "audit-recovery-steps/:uid", this.AuditRecovery)
	this.registerWebRequest(m, "agents", this.Agents)
	this.registerWebRequest(m, "agent/:host", this.Agent)
	this.registerWebRequest(m, "seed-details/:seedId", this.AgentSeedDetails)
	this.registerWebRequest(m, "seeds", this.Seeds)

	this.RegisterDebug(m)
}

// RegisterDebug adds handlers for /debug/vars (expvar) and /debug/pprof (net/http/pprof) support
func (this *HttpWeb) RegisterDebug(m *martini.ClassicMartini) {
	m.Get(this.URLPrefix+"/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		// from expvar.go, since the expvarHandler isn't exported :(
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		fmt.Fprintf(w, "{\n")
		first := true
		expvar.Do(func(kv expvar.KeyValue) {
			if !first {
				fmt.Fprintf(w, ",\n")
			}
			first = false
			fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
		})
		fmt.Fprintf(w, "\n}\n")
	})

	// list all the /debug/ endpoints we want
	m.Get(this.URLPrefix+"/debug/pprof", pprof.Index)
	m.Get(this.URLPrefix+"/debug/pprof/cmdline", pprof.Cmdline)
	m.Get(this.URLPrefix+"/debug/pprof/profile", pprof.Profile)
	m.Get(this.URLPrefix+"/debug/pprof/symbol", pprof.Symbol)
	m.Post(this.URLPrefix+"/debug/pprof/symbol", pprof.Symbol)
	m.Get(this.URLPrefix+"/debug/pprof/block", pprof.Handler("block").ServeHTTP)
	m.Get(this.URLPrefix+"/debug/pprof/heap", pprof.Handler("heap").ServeHTTP)
	m.Get(this.URLPrefix+"/debug/pprof/goroutine", pprof.Handler("goroutine").ServeHTTP)
	m.Get(this.URLPrefix+"/debug/pprof/threadcreate", pprof.Handler("threadcreate").ServeHTTP)

	// go-metrics
	m.Get(this.URLPrefix+"/debug/metrics", exp.ExpHandler(metrics.DefaultRegistry))
}
