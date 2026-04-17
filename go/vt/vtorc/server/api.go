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
	"vitess.io/vitess/go/viperutil/debug"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo/topoproto"
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
	errantGTIDsAPI             = "/api/errant-gtids"
	disableGlobalRecoveriesAPI = "/api/disable-global-recoveries"
	enableGlobalRecoveriesAPI  = "/api/enable-global-recoveries"
	detectionAnalysisAPI       = "/api/detection-analysis"
	databaseStateAPI           = "/api/database-state"
	configAPI                  = "/api/config"
	healthAPI                  = "/debug/health"

	shardWithoutKeyspaceFilteringErrorStr = "Filtering by shard without keyspace isn't supported"
	notAValidValueForSeconds              = "Invalid value for seconds"
)

var (
	apiHandler    = &vtorcAPI{}
	vtorcAPIPaths = []string{
		problemsAPI,
		errantGTIDsAPI,
		disableGlobalRecoveriesAPI,
		enableGlobalRecoveriesAPI,
		detectionAnalysisAPI,
		databaseStateAPI,
		configAPI,
		healthAPI,
	}
)

// ServeHTTP implements the http.Handler interface. This is the entry point for all the api commands of VTOrc
func (v *vtorcAPI) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	apiPath := request.URL.Path
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
	case errantGTIDsAPI:
		errantGTIDsAPIHandler(response, request)
	case detectionAnalysisAPI:
		detectionAnalysisAPIHandler(response, request)
	case databaseStateAPI:
		databaseStateAPIHandler(response)
	case configAPI:
		configAPIHandler(response)
	default:
		// This should be unreachable. Any endpoint which isn't registered is automatically redirected to /debug/status.
		// This code will only be reachable if we register an API but don't handle it here. That will be a bug.
		http.Error(response, "API registered but not handled. Please open an issue at https://github.com/vitessio/vitess/issues/new/choose", http.StatusInternalServerError)
	}
}

// getACLPermissionLevelForAPI returns the acl permission level that is required to run a given API
func getACLPermissionLevelForAPI(apiEndpoint string) string {
	switch apiEndpoint {
	case problemsAPI, errantGTIDsAPI:
		return acl.MONITORING
	case disableGlobalRecoveriesAPI, enableGlobalRecoveriesAPI:
		return acl.ADMIN
	case detectionAnalysisAPI, configAPI:
		return acl.MONITORING
	case healthAPI, databaseStateAPI:
		return acl.MONITORING
	}
	return acl.ADMIN
}

// RegisterVTOrcAPIEndpoints is used to register the VTOrc API endpoints
func RegisterVTOrcAPIEndpoints() {
	for _, apiPath := range vtorcAPIPaths {
		servenv.HTTPHandle(apiPath, apiHandler)
	}
}

// returnAsJSON returns the argument received on the responseWriter as a json object
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
	shard := request.URL.Query().Get("shard")
	keyspace := request.URL.Query().Get("keyspace")
	if shard != "" && keyspace == "" {
		http.Error(response, shardWithoutKeyspaceFilteringErrorStr, http.StatusBadRequest)
		return
	}
	instances, err := inst.ReadProblemInstances(keyspace, shard)
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	var processed []*LegacyInstanceJSON
	for _, i := range instances {
		processed = append(processed, &LegacyInstanceJSON{i})
	}
	returnAsJSON(response, http.StatusOK, processed)
}

// errantGTIDsAPIHandler is the handler for the errantGTIDsAPI endpoint
func errantGTIDsAPIHandler(response http.ResponseWriter, request *http.Request) {
	// This api also supports filtering by shard and keyspace provided.
	shard := request.URL.Query().Get("shard")
	keyspace := request.URL.Query().Get("keyspace")
	if shard != "" && keyspace == "" {
		http.Error(response, shardWithoutKeyspaceFilteringErrorStr, http.StatusBadRequest)
		return
	}

	instances, err := inst.ReadInstancesWithErrantGTIds(keyspace, shard)
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	var processed []*LegacyInstanceJSON
	for _, i := range instances {
		processed = append(processed, &LegacyInstanceJSON{i})
	}
	returnAsJSON(response, http.StatusOK, processed)
}

// databaseStateAPIHandler is the handler for the databaseStateAPI endpoint
func databaseStateAPIHandler(response http.ResponseWriter) {
	ds, err := inst.GetDatabaseState()
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	writePlainTextResponse(response, ds, http.StatusOK)
}

// configAPIHandler is the handler for the configAPI endpoint
func configAPIHandler(response http.ResponseWriter) {
	settingsMap := debug.AllSettings()
	jsonOut, err := json.MarshalIndent(settingsMap, "", "\t")
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	writePlainTextResponse(response, string(jsonOut), http.StatusOK)
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

// LegacyDetectionAnalysisJSON is a wrapper to *inst.DetectionAnalysis that
// provides the AnalyzedInstanceAlias field in the old string-based format.
type LegacyDetectionAnalysisJSON struct {
	*inst.DetectionAnalysis
}

// NewLegacyDetectionAnalysisJSON wraps a *inst.DetectionAnalysis with *LegacyDetectionAnalysisJSON.
func NewLegacyDetectionAnalysisJSON(da *inst.DetectionAnalysis) *LegacyDetectionAnalysisJSON {
	return &LegacyDetectionAnalysisJSON{da}
}

// MarshalJSON converts a *LegacyDetectionAnalysisJSON to the legacy format JSON,
// outputting AnalyzedInstanceAlias and AnalyzedInstancePrimaryAlias as strings.
func (ldaj *LegacyDetectionAnalysisJSON) MarshalJSON() ([]byte, error) {
	type plain inst.DetectionAnalysis
	data, err := json.Marshal((*plain)(ldaj.DetectionAnalysis))
	if err != nil {
		return nil, err
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if b, err := json.Marshal(topoproto.TabletAliasString(ldaj.AnalyzedInstanceAlias)); err == nil {
		m["AnalyzedInstanceAlias"] = b
	}
	if b, err := json.Marshal(topoproto.TabletAliasString(ldaj.AnalyzedInstancePrimaryAlias)); err == nil {
		m["AnalyzedInstancePrimaryAlias"] = b
	}
	return json.Marshal(m)
}

// LegacyInstanceJSON is a wrapper to *inst.Instance that provides the
// InstanceAlias field in the old string-based format.
type LegacyInstanceJSON struct {
	*inst.Instance
}

// MarshalJSON converts a *LegacyInstanceJSON to the legacy format JSON,
// outputting InstanceAlias as a string.
func (lij *LegacyInstanceJSON) MarshalJSON() ([]byte, error) {
	type plain inst.Instance
	data, err := json.Marshal((*plain)(lij.Instance))
	if err != nil {
		return nil, err
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if b, err := json.Marshal(topoproto.TabletAliasString(lij.InstanceAlias)); err == nil {
		m["InstanceAlias"] = b
	}
	return json.Marshal(m)
}

// detectionAnalysisAPIHandler is the handler for the detectionAnalysisAPI endpoint
func detectionAnalysisAPIHandler(response http.ResponseWriter, request *http.Request) {
	// This api also supports filtering by shard and keyspace provided.
	shard := request.URL.Query().Get("shard")
	keyspace := request.URL.Query().Get("keyspace")
	if shard != "" && keyspace == "" {
		http.Error(response, shardWithoutKeyspaceFilteringErrorStr, http.StatusBadRequest)
		return
	}
	analysis, err := inst.GetDetectionAnalysis(keyspace, shard, &inst.DetectionAnalysisHints{})
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	var processed []*LegacyDetectionAnalysisJSON
	for _, a := range analysis {
		processed = append(processed, NewLegacyDetectionAnalysisJSON(a))
	}
	returnAsJSON(response, http.StatusOK, processed)
}

// healthAPIHandler is the handler for the healthAPI endpoint
func healthAPIHandler(response http.ResponseWriter, request *http.Request) {
	health, discoveredOnce := process.HealthTest()
	code := http.StatusOK
	// If the process isn't healthy, or if the first discovery cycle hasn't completed, we return an internal server error.
	if !health.Healthy || !discoveredOnce {
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
