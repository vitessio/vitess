// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpctabletconn

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
)

func init() {
	tabletconn.AppendResultFuncMap[ProtocolBson] = AppendResultBson
	tabletconn.MergeResultsFuncMap[ProtocolBson] = MergeResultsBson
	tabletconn.MergeBatchResultsFuncMap[ProtocolBson] = MergeBatchResultsBson
}

func AppendResultBson(qr, innerqr interface{}) {
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

func MergeResultsBson(results <-chan interface{}) interface{} {
	res := new(mproto.QueryResult)
	for result := range results {
		AppendResultBson(res, result)
	}
	return res
}

func MergeBatchResultsBson(batchSize int, results <-chan interface{}) interface{} {
	qrs := new(tproto.QueryResultList)
	qrs.List = make([]mproto.QueryResult, batchSize)
	for innerqr := range results {
		innerqr := innerqr.(*tproto.QueryResultList)
		for i := range qrs.List {
			AppendResultBson(&qrs.List[i], &innerqr.List[i])
		}
	}
	return qrs
}
