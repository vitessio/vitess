/*
Copyright 2021 The Vitess Authors.

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

package experimental

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"text/template"

	vtadminhttp "vitess.io/vitess/go/vt/vtadmin/http"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// TabletDebugVarsPassthrough makes a passthrough request to a tablet's
// /debug/vars route, after looking up the tablet via VTAdmin's GetTablet
// rpc.
func TabletDebugVarsPassthrough(ctx context.Context, r vtadminhttp.Request, api *vtadminhttp.API) *vtadminhttp.JSONResponse {
	vars := r.Vars()

	alias, err := vars.GetTabletAlias("tablet")
	if err != nil {
		return vtadminhttp.NewJSONResponse(nil, err)
	}

	tablet, err := api.Server().GetTablet(ctx, &vtadminpb.GetTabletRequest{
		Alias:      alias,
		ClusterIds: r.URL.Query()["cluster"],
	})

	if err != nil {
		return vtadminhttp.NewJSONResponse(nil, err)
	}

	debugVars, err := getDebugVars(ctx, api, tablet)
	return vtadminhttp.NewJSONResponse(debugVars, err)
}

func getDebugVars(ctx context.Context, api *vtadminhttp.API, tablet *vtadminpb.Tablet) (map[string]any, error) {
	tmpl, err := template.New("tablet-fqdn").Parse(api.Options().ExperimentalOptions.TabletURLTmpl)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(nil)
	if err := tmpl.Execute(buf, tablet); err != nil {
		return nil, err
	}
	_, _ = buf.WriteString("/debug/vars")

	url := buf.String()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var debugVars map[string]any
	if err := json.Unmarshal(data, &debugVars); err != nil {
		return nil, err
	}

	return debugVars, nil
}
