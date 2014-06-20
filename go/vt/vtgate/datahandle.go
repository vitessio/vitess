// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
)

type AppendResultFunc func(qr, innerqr interface{})
type MergeResultsFunc func(<-chan interface{}) interface{}
type MergeBatchResultsFunc func(int, <-chan interface{}) interface{}

var (
	tabletConnProtocol       = ""
	appendResultFuncMap      = make(map[string]AppendResultFunc)
	mergeResultsFuncMap      = make(map[string]MergeResultsFunc)
	mergeBatchResultsFuncMap = make(map[string]MergeBatchResultsFunc)
)

func init() {
	tabletConnProtocol = tabletconn.GetTabletProtocol()
}

func appendResult(qr, innerqr interface{}) {
	appendResultFuncMap[tabletConnProtocol](qr, innerqr)
}

func mergeResults(results <-chan interface{}) interface{} {
	return mergeResultsFuncMap[tabletConnProtocol](results)
}

func mergeBatchResults(batchSize int, results <-chan interface{}) interface{} {
	return mergeBatchResultsFuncMap[tabletConnProtocol](batchSize, results)
}
