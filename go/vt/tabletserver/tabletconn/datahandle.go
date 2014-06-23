// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletconn

type AppendResultFunc func(qr, innerqr interface{})
type MergeResultsFunc func(<-chan interface{}) interface{}
type MergeBatchResultsFunc func(int, <-chan interface{}) interface{}

var (
	AppendResultFuncMap      = make(map[string]AppendResultFunc)
	MergeResultsFuncMap      = make(map[string]MergeResultsFunc)
	MergeBatchResultsFuncMap = make(map[string]MergeBatchResultsFunc)
)

func AppendResult(qr, innerqr interface{}) {
	AppendResultFuncMap[*tabletProtocol](qr, innerqr)
}

func MergeResults(results <-chan interface{}) interface{} {
	return MergeResultsFuncMap[*tabletProtocol](results)
}

func MergeBatchResults(batchSize int, results <-chan interface{}) interface{} {
	return MergeBatchResultsFuncMap[*tabletProtocol](batchSize, results)
}
