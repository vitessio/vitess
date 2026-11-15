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

package tabletserver

import (
	"context"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/resultcharset"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// A tablet started with --queryserver-config-raw-result-charset reads the
// results of the queries of clients with character_set_results = NULL (see
// tabletenv.TabletConfig.QueryServingConnector). MySQL then sends the text of
// each column in the character set of the column, with the collation and the
// length of the column in the field: a latin1_german2_ci VARCHAR(10) with
// 'Müller' arrives as 4D FC 6C 6C 65 72 with the collation 31 and the length
// 10.
//
// A caller that marks its request with queryservice.ContextWithRawResultCharset
// converts the text itself, and gets the result as MySQL sent it, except that
// the columns in the connection character set get the length MySQL sends for
// them with character_set_results set (see resultcharset.ConnectionLengths).
// Every other caller gets the result converted with resultcharset.ToConnection
// as well, which gives what MySQL sends with character_set_results set to the
// connection character set: 4D C3 BC 6C 6C 65 72 with the collation 255 and the
// length 40 in the example.
//
// The conversion happens per caller, after the consolidator has handed out a
// result that it shares, and never changes the result it is given.
//
// A reserved connection on which the client changed character_set_results
// with SET (see StatefulConnection.resultCharsetSet) sends results the way the
// client asked for them, and its results are returned as MySQL sent them.

// convertsResult reports whether the result of a request with ctx and options
// is converted to the connection character set before it is returned.
//
// That is the case on a tablet that reads with character_set_results = NULL,
// unless the caller converts the text itself and gets the collations it needs
// for that: a result without the full fields (included_fields other than ALL)
// has no collations, and is converted here.
func (tsv *TabletServer) convertsResult(ctx context.Context, options *querypb.ExecuteOptions) bool {
	if !tsv.config.RawResultCharset {
		return false
	}
	return !queryservice.IsRawResultCharset(ctx) || sqltypes.IncludeFieldsOrDefault(options) != querypb.ExecuteOptions_ALL
}

// connectionCollation returns the collation that MySQL reports in the fields of
// text columns on the tablet's connections with character_set_results set: the
// collation the connections negotiated, which the session setup names. That is
// utf8mb4_0900_ai_ci (255) for the default --db-charset utf8mb4 on MySQL 8.0
// and 8.4, and utf8mb4_general_ci (45) for --db-charset utf8mb4_general_ci.
//
// A tablet configured with no character set uses the one the server announces,
// which is not known here; it is taken to be the default connection collation
// of the tablet's MySQL version.
func (tsv *TabletServer) connectionCollation() collations.ID {
	if coll := collations.ID(tsv.connCollation.Load()); coll != collations.Unknown {
		return coll
	}
	return tsv.env.CollationEnv().DefaultConnectionCharset()
}

// initConnectionCollation records the collation of the tablet's connections
// for connectionCollation.
func (tsv *TabletServer) initConnectionCollation() {
	tsv.connCollation.Store(uint32(tsv.config.DB.AppWithDB().Charset()))
}

// convertResult returns the result that a query of a request with ctx and
// options read from MySQL, for its caller. asSent is true for a connection
// that sends results the way its client asked for them.
func (tsv *TabletServer) convertResult(ctx context.Context, options *querypb.ExecuteOptions, asSent bool, qr *sqltypes.Result) *sqltypes.Result {
	if !tsv.config.RawResultCharset || asSent || qr == nil {
		return qr
	}
	conn := tsv.connectionCollation()
	qr = resultcharset.ConnectionLengths(qr.Fields, conn).Result(qr)
	if tsv.convertsResult(ctx, options) {
		qr = resultcharset.ToConnection(qr.Fields, conn, true).Result(qr)
	}
	return qr
}

// convertStream returns the callback for the packets of a streamed result
// that convertResult converts, and that it then strips to the fields that the
// caller asked for in options: a streamed query reads the full fields on a
// tablet that reads with character_set_results = NULL, since the conversion
// needs the collations (see QueryExecutor.streamIncludedFields). asSent is
// called when the fields arrive.
func (tsv *TabletServer) convertStream(ctx context.Context, options *querypb.ExecuteOptions, asSent func() bool, callback func(*sqltypes.Result) error) func(*sqltypes.Result) error {
	if !tsv.config.RawResultCharset {
		return callback
	}
	conn := tsv.connectionCollation()
	convert := tsv.convertsResult(ctx, options)
	var raw bool
	lengths := resultcharset.NewStream(func(fields []*querypb.Field) *resultcharset.Conversion {
		raw = !asSent()
		if !raw {
			return &resultcharset.Conversion{}
		}
		return resultcharset.ConnectionLengths(fields, conn)
	})
	text := resultcharset.NewStream(func(fields []*querypb.Field) *resultcharset.Conversion {
		if !raw || !convert {
			return &resultcharset.Conversion{}
		}
		return resultcharset.ToConnection(fields, conn, true)
	})
	included := sqltypes.IncludeFieldsOrDefault(options)
	return func(qr *sqltypes.Result) error {
		if qr == nil {
			return callback(qr)
		}
		return callback(text.Result(lengths.Result(qr)).StripMetadata(included))
	}
}
