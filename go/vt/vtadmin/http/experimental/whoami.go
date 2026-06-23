/*
Copyright 2026 The Vitess Authors.

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
	"context"

	vtadminhttp "vitess.io/vitess/go/vt/vtadmin/http"
	"vitess.io/vitess/go/vt/vtadmin/rbac"
)

// WhoAmI is an experimental route for extracting authenticated Actors from
// the request, to see who is authenticated on the frontend.
func WhoAmI(ctx context.Context, r vtadminhttp.Request, api *vtadminhttp.API) *vtadminhttp.JSONResponse {
	data := map[string]any{}
	actor, ok := rbac.FromContext(ctx)
	data["authenticated"] = ok
	if ok {
		data["actor"] = actor
	}

	return vtadminhttp.NewJSONResponse(data, nil)
}
