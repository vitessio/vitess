// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

type AppendResultFunc func(qr, innerqr interface{})
type MergeResultsFunc func(<-chan interface{}) interface{}
type MergeBatchResultsFunc func(int, <-chan interface{}) interface{}

var (
	appendResultFuncMap      = make(map[string]AppendResultFunc)
	mergeResultsFuncMap      = make(map[string]MergeResultsFunc)
	mergeBatchResultsFuncMap = make(map[string]MergeBatchResultsFunc)
)

func appendResult(protocol string, qr, innerqr interface{}) {
	appendResultFuncMap[protocol](qr, innerqr)
}

func mergeResults(protocol string, results <-chan interface{}) interface{} {
	return mergeResultsFuncMap[protocol](results)
}

func mergeBatchResults(protocol string, batchSize int, results <-chan interface{}) interface{} {
	return mergeBatchResultsFuncMap[protocol](batchSize, results)
}
