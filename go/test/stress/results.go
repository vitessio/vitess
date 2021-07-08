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

package stress

import "fmt"

type (
	queryCount struct {
		success int
		failure int
	}

	Result struct {
		selects, inserts, deletes queryCount
	}
)

func (qc queryCount) successQPS(seconds float64) int {
	return qc.success / int(seconds)
}

func (qc queryCount) failureQPS(seconds float64) int {
	return qc.failure / int(seconds)
}

func (qc queryCount) totalQPS(seconds float64) int {
	return qc.successQPS(seconds) + qc.failureQPS(seconds)
}

func (qc queryCount) sum() int {
	return qc.success + qc.failure
}

func sumQueryCounts(qcs ...queryCount) queryCount {
	var qc queryCount
	for _, qci := range qcs {
		qc.success += qci.success
		qc.failure += qci.failure
	}
	return qc
}

func (r Result) Print(seconds float64) {
	allQCs := sumQueryCounts(r.selects, r.inserts, r.deletes)
	fmt.Printf(`QPS:
	select: %d, failed: %d, sum: %d
	insert: %d, failed: %d, sum: %d
	delete: %d, failed: %d, sum: %d
	---------
	total:	%d, failed: %d, sum: %d
	
Queries:
	select: %d, failed: %d, sum: %d
	insert: %d, failed: %d, sum: %d
	delete: %d, failed: %d, sum: %d
	---------
	total:	%d, failed: %d, sum: %d
	
`, r.selects.successQPS(seconds), r.selects.failureQPS(seconds), r.selects.totalQPS(seconds),
		r.inserts.successQPS(seconds), r.inserts.failureQPS(seconds), r.inserts.totalQPS(seconds),
		r.deletes.successQPS(seconds), r.deletes.failureQPS(seconds), r.deletes.totalQPS(seconds),
		allQCs.successQPS(seconds), allQCs.failureQPS(seconds), allQCs.totalQPS(seconds),
		r.selects.success, r.selects.failure, r.selects.sum(),
		r.inserts.success, r.inserts.failure, r.inserts.sum(),
		r.deletes.success, r.deletes.failure, r.deletes.sum(),
		allQCs.success, allQCs.failure, allQCs.sum())
}
