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

package vtgate

import (
	"context"

	"vitess.io/vitess/go/vt/vttablet/queryservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// With --convert-result-charset, vtgate tells every tablet that it converts
// the text of results itself.
//
// A tablet started with --queryserver-config-raw-result-charset then returns
// the text of each column as MySQL sends it with character_set_results = NULL:
// a latin1_swedish_ci VARCHAR(10) with 'Müller' arrives as 4D FC 6C 6C 65 72
// with the collation 8 and the length 10. vtgate keeps it so: inside vtgate
// the text of a value of a row is in the character set of the collation in
// its field. It evaluates the text in that collation, and converts it once,
// to the character_set_results of the client, when it returns the result (see
// VTGate.toClient). What vtgate puts into a statement of its own, such as the
// value of a join for the other side of the join, it converts to the
// connection character set first, which is the one of every statement.
//
// A tablet without the flag, or one that predates it, returns every text
// column in its connection character set with its connection collation. That
// result says of itself that it is utf8mb4, and is handled the same way.
//
// Without --convert-result-charset, vtgate marks no request, so a tablet with
// the flag converts its results itself, and vtgate returns what the tablet
// sent, as before.
//
// The mark is set in the TabletGateway, which every query that vtgate sends to
// a tablet goes through: the QueryService of the gateway and the ones of
// QueryServiceByAlias.

// markRawResultCharset returns a wrapper that, with --convert-result-charset,
// marks the context of every request with
// queryservice.ContextWithRawResultCharset before it calls wrapper.
func markRawResultCharset(wrapper queryservice.WrapperFunc) queryservice.WrapperFunc {
	return func(ctx context.Context, target *querypb.Target, conn queryservice.QueryService, name string, opts queryservice.WrapOpts, inner func(context.Context, *querypb.Target, queryservice.QueryService) (bool, error)) error {
		if convertResultCharset {
			ctx = queryservice.ContextWithRawResultCharset(ctx)
		}
		return wrapper(ctx, target, conn, name, opts, inner)
	}
}
