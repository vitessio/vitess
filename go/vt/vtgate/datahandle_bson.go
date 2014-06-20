// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
)

// ProtocolBson is the type used for tablet connection.
// It should match the name in gorpctabletconn/conn.go.
const ProtocolBson = "gorpc"

func init() {
	appendResultFuncMap[ProtocolBson] = appendResultBson
	mergeResultsFuncMap[ProtocolBson] = mergeResultsBson
	mergeBatchResultsFuncMap[ProtocolBson] = mergeBatchResultsBson
}

func appendResultBson(qr, innerqr interface{}) {
	to := qr.(*mproto.QueryResult)
	from := innerqr.(*mproto.QueryResult)
	if from.RowsAffected == 0 && len(from.Fields) == 0 {
		return
	}
	if to.Fields == nil {
		to.Fields = from.Fields
	}
	to.RowsAffected += from.RowsAffected
	if from.InsertId != 0 {
		to.InsertId = from.InsertId
	}
	to.Rows = append(to.Rows, from.Rows...)
}

func mergeResultsBson(results <-chan interface{}) interface{} {
	res := new(mproto.QueryResult)
	for result := range results {
		result := result.(*mproto.QueryResult)
		if result.RowsAffected == 0 && len(result.Fields) == 0 {
			continue
		}
		if res.Fields == nil {
			res.Fields = result.Fields
		}
		res.RowsAffected += result.RowsAffected
		if result.InsertId != 0 {
			res.InsertId = result.InsertId
		}
		res.Rows = append(res.Rows, result.Rows...)
	}
	return res
}

func mergeBatchResultsBson(batchSize int, results <-chan interface{}) interface{} {
	qrs := new(tproto.QueryResultList)
	qrs.List = make([]mproto.QueryResult, batchSize)
	for innerqr := range results {
		innerqr := innerqr.(*tproto.QueryResultList)
		for i := range qrs.List {
			appendResultBson(&qrs.List[i], &innerqr.List[i])
		}
	}
	return qrs
}
