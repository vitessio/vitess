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

	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

// Web is the web requests server, mapping each request to a web page
type Web struct {
	URLPrefix string
}

var HTTPWeb = Web{}

func (httpWeb *Web) getInstanceKey(host string, port string) (inst.InstanceKey, error) {
	instanceKey := inst.InstanceKey{Hostname: host}
	var err error

	if instanceKey.Port, err = strconv.Atoi(port); err != nil {
		return instanceKey, fmt.Errorf("Invalid port: %s", port)
	}
	return instanceKey, err
}

func (httpWeb *Web) AccessToken(params martini.Params, r render.Render, req *http.Request, resp http.ResponseWriter, user auth.User) {
	publicToken := template.JSEscapeString(req.URL.Query().Get("publicToken"))
	err := authenticateToken(publicToken, resp)
	if err != nil {
		r.JSON(200, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	r.Redirect(httpWeb.URLPrefix + "/")
}

func (httpWeb *Web) Index(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	// Redirect index so that all web URLs begin with "/web/".
	// We also redirect /web/ to /web/clusters so that
	// the Clusters page has a single canonical URL.
	r.Redirect(httpWeb.URLPrefix + "/web/clusters")
}

func (httpWeb *Web) Clusters(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/clusters", map[string]any{
		"agentsHttpActive":              config.Config.ServeAgentsHTTP,
		"title":                         "clusters",
		"autoshow_problems":             false,
		"authorizedForAction":           isAuthorizedForAction(req, user),
		"userId":                        getUserID(req, user),
		"removeTextFromHostnameDisplay": config.Config.RemoveTextFromHostnameDisplay,
		"prefix":                        httpWeb.URLPrefix,
		"webMessage":                    config.Config.WebMessage,
	})
}

func (httpWeb *Web) ClustersAnalysis(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/clusters_analysis", map[string]any{
		"agentsHttpActive":              config.Config.ServeAgentsHTTP,
		"title":                         "clusters",
		"autoshow_problems":             false,
		"authorizedForAction":           isAuthorizedForAction(req, user),
		"userId":                        getUserID(req, user),
		"removeTextFromHostnameDisplay": config.Config.RemoveTextFromHostnameDisplay,
		"prefix":                        httpWeb.URLPrefix,
		"webMessage":                    config.Config.WebMessage,
	})
}

func (httpWeb *Web) Cluster(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	clusterName, _ := figureClusterName(params["clusterName"])

	r.HTML(200, "templates/cluster", map[string]any{
		"agentsHttpActive":              config.Config.ServeAgentsHTTP,
		"title":                         "cluster",
		"clusterName":                   clusterName,
		"autoshow_problems":             true,
		"contextMenuVisible":            true,
		"authorizedForAction":           isAuthorizedForAction(req, user),
		"userId":                        getUserID(req, user),
		"removeTextFromHostnameDisplay": config.Config.RemoveTextFromHostnameDisplay,
		"compactDisplay":                template.JSEscapeString(req.URL.Query().Get("compact")),
		"prefix":                        httpWeb.URLPrefix,
		"webMessage":                    config.Config.WebMessage,
	})
}

func (httpWeb *Web) ClusterByAlias(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	params["clusterName"] = params["clusterAlias"]
	httpWeb.Cluster(params, r, req, user)
}

func (httpWeb *Web) ClusterByInstance(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	instanceKey, err := httpWeb.getInstanceKey(params["host"], params["port"])
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
	httpWeb.Cluster(params, r, req, user)
}

func (httpWeb *Web) ClusterPools(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	clusterName, _ := figureClusterName(params["clusterName"])
	r.HTML(200, "templates/cluster_pools", map[string]any{
		"agentsHttpActive":              config.Config.ServeAgentsHTTP,
		"title":                         "cluster pools",
		"clusterName":                   clusterName,
		"autoshow_problems":             false, // because pool screen by default expands all hosts
		"contextMenuVisible":            true,
		"authorizedForAction":           isAuthorizedForAction(req, user),
		"userId":                        getUserID(req, user),
		"removeTextFromHostnameDisplay": config.Config.RemoveTextFromHostnameDisplay,
		"compactDisplay":                template.JSEscapeString(req.URL.Query().Get("compact")),
		"prefix":                        httpWeb.URLPrefix,
		"webMessage":                    config.Config.WebMessage,
	})
}

func (httpWeb *Web) Search(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	searchString := params["searchString"]
	if searchString == "" {
		searchString = req.URL.Query().Get("s")
	}
	searchString = template.JSEscapeString(searchString)
	r.HTML(200, "templates/search", map[string]any{
		"agentsHttpActive":    config.Config.ServeAgentsHTTP,
		"title":               "search",
		"searchString":        searchString,
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserID(req, user),
		"autoshow_problems":   false,
		"prefix":              httpWeb.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (httpWeb *Web) Discover(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/discover", map[string]any{
		"agentsHttpActive":    config.Config.ServeAgentsHTTP,
		"title":               "discover",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserID(req, user),
		"autoshow_problems":   false,
		"prefix":              httpWeb.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (httpWeb *Web) Audit(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	page, err := strconv.Atoi(params["page"])
	if err != nil {
		page = 0
	}

	r.HTML(200, "templates/audit", map[string]any{
		"agentsHttpActive":    config.Config.ServeAgentsHTTP,
		"title":               "audit",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserID(req, user),
		"autoshow_problems":   false,
		"page":                page,
		"auditHostname":       params["host"],
		"auditPort":           params["port"],
		"prefix":              httpWeb.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (httpWeb *Web) AuditRecovery(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	page, err := strconv.Atoi(params["page"])
	if err != nil {
		page = 0
	}
	recoveryID, err := strconv.ParseInt(params["id"], 10, 0)
	if err != nil {
		recoveryID = 0
	}
	recoveryUID := params["uid"]
	clusterAlias := params["clusterAlias"]

	clusterName, _ := figureClusterName(params["clusterName"])
	r.HTML(200, "templates/audit_recovery", map[string]any{
		"agentsHttpActive":    config.Config.ServeAgentsHTTP,
		"title":               "audit-recovery",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserID(req, user),
		"autoshow_problems":   false,
		"page":                page,
		"clusterName":         clusterName,
		"clusterAlias":        clusterAlias,
		"recoveryId":          recoveryID,
		"recoveryUid":         recoveryUID,
		"prefix":              httpWeb.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (httpWeb *Web) AuditFailureDetection(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	page, err := strconv.Atoi(params["page"])
	if err != nil {
		page = 0
	}
	detectionID, err := strconv.ParseInt(params["id"], 10, 0)
	if err != nil {
		detectionID = 0
	}
	clusterAlias := params["clusterAlias"]

	r.HTML(200, "templates/audit_failure_detection", map[string]any{
		"agentsHttpActive":    config.Config.ServeAgentsHTTP,
		"title":               "audit-failure-detection",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserID(req, user),
		"autoshow_problems":   false,
		"page":                page,
		"detectionId":         detectionID,
		"clusterAlias":        clusterAlias,
		"prefix":              httpWeb.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (httpWeb *Web) Agents(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/agents", map[string]any{
		"agentsHttpActive":    config.Config.ServeAgentsHTTP,
		"title":               "agents",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserID(req, user),
		"autoshow_problems":   false,
		"prefix":              httpWeb.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (httpWeb *Web) Agent(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/agent", map[string]any{
		"agentsHttpActive":    config.Config.ServeAgentsHTTP,
		"title":               "agent",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserID(req, user),
		"autoshow_problems":   false,
		"agentHost":           params["host"],
		"prefix":              httpWeb.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (httpWeb *Web) AgentSeedDetails(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/agent_seed_details", map[string]any{
		"agentsHttpActive":    config.Config.ServeAgentsHTTP,
		"title":               "agent seed details",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserID(req, user),
		"autoshow_problems":   false,
		"seedId":              params["seedId"],
		"prefix":              httpWeb.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (httpWeb *Web) Seeds(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	r.HTML(200, "templates/seeds", map[string]any{
		"agentsHttpActive":    config.Config.ServeAgentsHTTP,
		"title":               "seeds",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserID(req, user),
		"autoshow_problems":   false,
		"prefix":              httpWeb.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (httpWeb *Web) Home(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/home", map[string]any{
		"agentsHttpActive":    config.Config.ServeAgentsHTTP,
		"title":               "home",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserID(req, user),
		"autoshow_problems":   false,
		"prefix":              httpWeb.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (httpWeb *Web) About(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/about", map[string]any{
		"agentsHttpActive":    config.Config.ServeAgentsHTTP,
		"title":               "about",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserID(req, user),
		"autoshow_problems":   false,
		"prefix":              httpWeb.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (httpWeb *Web) KeepCalm(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/keep-calm", map[string]any{
		"agentsHttpActive":    config.Config.ServeAgentsHTTP,
		"title":               "Keep Calm",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserID(req, user),
		"autoshow_problems":   false,
		"prefix":              httpWeb.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (httpWeb *Web) FAQ(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/faq", map[string]any{
		"agentsHttpActive":    config.Config.ServeAgentsHTTP,
		"title":               "FAQ",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserID(req, user),
		"autoshow_problems":   false,
		"prefix":              httpWeb.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (httpWeb *Web) Status(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	r.HTML(200, "templates/status", map[string]any{
		"agentsHttpActive":    config.Config.ServeAgentsHTTP,
		"title":               "status",
		"authorizedForAction": isAuthorizedForAction(req, user),
		"userId":              getUserID(req, user),
		"autoshow_problems":   false,
		"prefix":              httpWeb.URLPrefix,
		"webMessage":          config.Config.WebMessage,
	})
}

func (httpWeb *Web) registerWebRequest(m *martini.ClassicMartini, path string, handler martini.Handler) {
	fullPath := fmt.Sprintf("%s/web/%s", httpWeb.URLPrefix, path)
	if path == "/" {
		fullPath = fmt.Sprintf("%s/", httpWeb.URLPrefix)
	}

	m.Get(fullPath, handler)
}

// RegisterRequests makes for the de-facto list of known Web calls
func (httpWeb *Web) RegisterRequests(m *martini.ClassicMartini) {
	httpWeb.registerWebRequest(m, "access-token", httpWeb.AccessToken)
	httpWeb.registerWebRequest(m, "", httpWeb.Index)
	httpWeb.registerWebRequest(m, "/", httpWeb.Index)
	httpWeb.registerWebRequest(m, "home", httpWeb.About)
	httpWeb.registerWebRequest(m, "about", httpWeb.About)
	httpWeb.registerWebRequest(m, "keep-calm", httpWeb.KeepCalm)
	httpWeb.registerWebRequest(m, "faq", httpWeb.FAQ)
	httpWeb.registerWebRequest(m, "status", httpWeb.Status)
	httpWeb.registerWebRequest(m, "clusters", httpWeb.Clusters)
	httpWeb.registerWebRequest(m, "clusters-analysis", httpWeb.ClustersAnalysis)
	httpWeb.registerWebRequest(m, "cluster/:clusterName", httpWeb.Cluster)
	httpWeb.registerWebRequest(m, "cluster/alias/:clusterAlias", httpWeb.ClusterByAlias)
	httpWeb.registerWebRequest(m, "cluster/instance/:host/:port", httpWeb.ClusterByInstance)
	httpWeb.registerWebRequest(m, "cluster-pools/:clusterName", httpWeb.ClusterPools)
	httpWeb.registerWebRequest(m, "search/:searchString", httpWeb.Search)
	httpWeb.registerWebRequest(m, "search", httpWeb.Search)
	httpWeb.registerWebRequest(m, "discover", httpWeb.Discover)
	httpWeb.registerWebRequest(m, "audit", httpWeb.Audit)
	httpWeb.registerWebRequest(m, "audit/:page", httpWeb.Audit)
	httpWeb.registerWebRequest(m, "audit/instance/:host/:port", httpWeb.Audit)
	httpWeb.registerWebRequest(m, "audit/instance/:host/:port/:page", httpWeb.Audit)
	httpWeb.registerWebRequest(m, "audit-recovery", httpWeb.AuditRecovery)
	httpWeb.registerWebRequest(m, "audit-recovery/:page", httpWeb.AuditRecovery)
	httpWeb.registerWebRequest(m, "audit-recovery/id/:id", httpWeb.AuditRecovery)
	httpWeb.registerWebRequest(m, "audit-recovery/uid/:uid", httpWeb.AuditRecovery)
	httpWeb.registerWebRequest(m, "audit-recovery/cluster/:clusterName", httpWeb.AuditRecovery)
	httpWeb.registerWebRequest(m, "audit-recovery/cluster/:clusterName/:page", httpWeb.AuditRecovery)
	httpWeb.registerWebRequest(m, "audit-recovery/alias/:clusterAlias", httpWeb.AuditRecovery)
	httpWeb.registerWebRequest(m, "audit-recovery/alias/:clusterAlias/:page", httpWeb.AuditRecovery)
	httpWeb.registerWebRequest(m, "audit-failure-detection", httpWeb.AuditFailureDetection)
	httpWeb.registerWebRequest(m, "audit-failure-detection/:page", httpWeb.AuditFailureDetection)
	httpWeb.registerWebRequest(m, "audit-failure-detection/id/:id", httpWeb.AuditFailureDetection)
	httpWeb.registerWebRequest(m, "audit-failure-detection/alias/:clusterAlias", httpWeb.AuditFailureDetection)
	httpWeb.registerWebRequest(m, "audit-failure-detection/alias/:clusterAlias/:page", httpWeb.AuditFailureDetection)
	httpWeb.registerWebRequest(m, "audit-recovery-steps/:uid", httpWeb.AuditRecovery)
	httpWeb.registerWebRequest(m, "agents", httpWeb.Agents)
	httpWeb.registerWebRequest(m, "agent/:host", httpWeb.Agent)
	httpWeb.registerWebRequest(m, "seed-details/:seedId", httpWeb.AgentSeedDetails)
	httpWeb.registerWebRequest(m, "seeds", httpWeb.Seeds)

	httpWeb.RegisterDebug(m)
}

// RegisterDebug adds handlers for /debug/vars (expvar) and /debug/pprof (net/http/pprof) support
func (httpWeb *Web) RegisterDebug(m *martini.ClassicMartini) {
	m.Get(httpWeb.URLPrefix+"/debug/vars", func(w http.ResponseWriter, r *http.Request) {
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
	m.Get(httpWeb.URLPrefix+"/debug/pprof", pprof.Index)
	m.Get(httpWeb.URLPrefix+"/debug/pprof/cmdline", pprof.Cmdline)
	m.Get(httpWeb.URLPrefix+"/debug/pprof/profile", pprof.Profile)
	m.Get(httpWeb.URLPrefix+"/debug/pprof/symbol", pprof.Symbol)
	m.Post(httpWeb.URLPrefix+"/debug/pprof/symbol", pprof.Symbol)
	m.Get(httpWeb.URLPrefix+"/debug/pprof/block", pprof.Handler("block").ServeHTTP)
	m.Get(httpWeb.URLPrefix+"/debug/pprof/heap", pprof.Handler("heap").ServeHTTP)
	m.Get(httpWeb.URLPrefix+"/debug/pprof/goroutine", pprof.Handler("goroutine").ServeHTTP)
	m.Get(httpWeb.URLPrefix+"/debug/pprof/threadcreate", pprof.Handler("threadcreate").ServeHTTP)

	// go-metrics
	m.Get(httpWeb.URLPrefix+"/debug/metrics", exp.ExpHandler(metrics.DefaultRegistry))
}
