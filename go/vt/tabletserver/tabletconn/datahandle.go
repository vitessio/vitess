// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletconn

// AppendResultFunc defines the fucntion type appending result to another.
type AppendResultFunc func(qr, innerqr interface{})

// MergeResultsFunc defines the function type merging results to one.
type MergeResultsFunc func(<-chan interface{}) interface{}

// MergeBatchResultsFunc defines the function type merging batch results to one.
type MergeBatchResultsFunc func(int, <-chan interface{}) interface{}

var (
	// AppendResultFuncMap maps protocol to the func appending result.
	AppendResultFuncMap = make(map[string]AppendResultFunc)
	// MergeResultsFuncMap maps protocol to the func merging results.
	MergeResultsFuncMap = make(map[string]MergeResultsFunc)
	// MergeBatchResultsFuncMap maps protocol to the func merging batch results.
	MergeBatchResultsFuncMap = make(map[string]MergeBatchResultsFunc)
)

// AppendResult calls actual implemention based on tablet conn protocol.
func AppendResult(qr, innerqr interface{}) {
	AppendResultFuncMap[*tabletProtocol](qr, innerqr)
}

// MergeResults calls actual implementation based on tablet conn protocol.
func MergeResults(results <-chan interface{}) interface{} {
	return MergeResultsFuncMap[*tabletProtocol](results)
}

// MergeBatchResults calls actual implementation based on tablet conn protocol.
func MergeBatchResults(batchSize int, results <-chan interface{}) interface{} {
	return MergeBatchResultsFuncMap[*tabletProtocol](batchSize, results)
}
