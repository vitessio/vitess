/*
Copyright 2022 The Vitess Authors.

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

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/logic"
	"vitess.io/vitess/go/vt/vtorc/process"
)

// vtorcAPI struct is created to implement the Handler interface to register
// the API endpoints for VTOrc. Instead, we could have used the HandleFunc method
// of registering the endpoints, but this approach seems cleaner and easier to unit test
// as it abstracts the acl check code into a single place
type vtorcAPI struct{}

const (
	problemsAPI                = "/api/problems"
	disableGlobalRecoveriesAPI = "/api/disable-global-recoveries"
	enableGlobalRecoveriesAPI  = "/api/enable-global-recoveries"
	replicationAnalysisAPI     = "/api/replication-analysis"
	healthAPI                  = "/debug/health"
)

var (
	apiHandler    = &vtorcAPI{}
	vtorcAPIPaths = []string{
		problemsAPI,
		disableGlobalRecoveriesAPI,
		enableGlobalRecoveriesAPI,
		replicationAnalysisAPI,
		healthAPI,
	}
)

// ServeHTTP implements the http.Handler interface. This is the entry point for all the api commands of VTOrc
func (v *vtorcAPI) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	apiPath := request.URL.Path
	log.Infof("HTTP API Request received: %v", apiPath)
	if err := acl.CheckAccessHTTP(request, getACLPermissionLevelForAPI(apiPath)); err != nil {
		acl.SendError(response, err)
		return
	}

	switch apiPath {
	case disableGlobalRecoveriesAPI:
		disableGlobalRecoveriesAPIHandler(response)
	case enableGlobalRecoveriesAPI:
		enableGlobalRecoveriesAPIHandler(response)
	case healthAPI:
		healthAPIHandler(response, request)
	case problemsAPI:
		problemsAPIHandler(response, request)
	case replicationAnalysisAPI:
		replicationAnalysisAPIHandler(response, request)
	default:
		// This should be unreachable. Any endpoint which isn't registered is automatically redirected to /debug/status.
		// This code will only be reachable if we register an API but don't handle it here. That will be a bug.
		http.Error(response, "API registered but not handled. Please open an issue at https://github.com/vitessio/vitess/issues/new/choose", http.StatusInternalServerError)
	}
}

// getACLPermissionLevelForAPI returns the acl permission level that is required to run a given API
func getACLPermissionLevelForAPI(apiEndpoint string) string {
	switch apiEndpoint {
	case problemsAPI:
		return acl.MONITORING
	case disableGlobalRecoveriesAPI, enableGlobalRecoveriesAPI:
		return acl.ADMIN
	case replicationAnalysisAPI:
		return acl.MONITORING
	case healthAPI:
		return acl.MONITORING
	}
	return acl.ADMIN
}

// RegisterVTOrcAPIEndpoints is used to register the VTOrc API endpoints
func RegisterVTOrcAPIEndpoints() {
	for _, apiPath := range vtorcAPIPaths {
		http.Handle(apiPath, apiHandler)
	}
}

// returnAsJSON returns the argument received on the resposeWriter as a json object
func returnAsJSON(response http.ResponseWriter, code int, stuff any) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	response.WriteHeader(code)
	buf, err := json.MarshalIndent(stuff, "", " ")
	if err != nil {
		_, _ = response.Write([]byte(err.Error()))
		return
	}
	ebuf := bytes.NewBuffer(nil)
	json.HTMLEscape(ebuf, buf)
	_, _ = response.Write(ebuf.Bytes())
}

// problemsAPIHandler is the handler for the problemsAPI endpoint
func problemsAPIHandler(response http.ResponseWriter, request *http.Request) {
	// This api also supports filtering by shard and keyspace provided.
	// Currently, both of them have to be provided in order to filter the instances.
	// Once we split the cluster_name field into keyspace and shard, we can support
	// filtering just by keyspace as well.
	shard := request.URL.Query().Get("shard")
	keyspace := request.URL.Query().Get("keyspace")
	clusterName := ""
	// Override the cluster name to filter by only when both the parameters
	// are specified and not empty
	if keyspace != "" && shard != "" {
		clusterName = inst.GetClusterNameFromKeyspaceAndShard(keyspace, shard)
	}
	instances, err := inst.ReadProblemInstances(clusterName)
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	returnAsJSON(response, http.StatusOK, instances)
}

// disableGlobalRecoveriesAPIHandler is the handler for the disableGlobalRecoveriesAPI endpoint
func disableGlobalRecoveriesAPIHandler(response http.ResponseWriter) {
	err := logic.DisableRecovery()
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	writePlainTextResponse(response, "Global recoveries disabled", http.StatusOK)
}

// enableGlobalRecoveriesAPIHandler is the handler for the enableGlobalRecoveriesAPI endpoint
func enableGlobalRecoveriesAPIHandler(response http.ResponseWriter) {
	err := logic.EnableRecovery()
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	writePlainTextResponse(response, "Global recoveries enabled", http.StatusOK)
}

// replicationAnalysisAPIHandler is the handler for the replicationAnalysisAPI endpoint
func replicationAnalysisAPIHandler(response http.ResponseWriter, request *http.Request) {
	// This api also supports filtering by shard and keyspace provided.
	// Currently, both of them have to be provided in order to filter the replication analysis.
	// Once we split the cluster_name field into keyspace and shard, we can support
	// filtering just by keyspace as well.
	shard := request.URL.Query().Get("shard")
	keyspace := request.URL.Query().Get("keyspace")
	clusterName := ""
	// Override the cluster name to filter by only when both the parameters
	// are specified and not empty
	if keyspace != "" && shard != "" {
		clusterName = inst.GetClusterNameFromKeyspaceAndShard(keyspace, shard)
	}
	analysis, err := inst.GetReplicationAnalysis(clusterName, &inst.ReplicationAnalysisHints{})
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}

	// TODO: We can also add filtering for a specific instance too based on the tablet alias.
	// Currently inst.ReplicationAnalysis doesn't store the tablet alias, but once it does we can filter on that too
	returnAsJSON(response, http.StatusOK, analysis)
}

// healthAPIHandler is the handler for the healthAPI endpoint
func healthAPIHandler(response http.ResponseWriter, request *http.Request) {
	health, err := process.HealthTest()
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	code := http.StatusOK
	if !health.Healthy {
		code = http.StatusInternalServerError
	}
	returnAsJSON(response, code, health)
}

// writePlainTextResponse writes a plain text response to the writer.
func writePlainTextResponse(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_, _ = fmt.Fprintln(w, message)
}
