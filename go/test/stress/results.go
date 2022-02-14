package stress

type (
	queryCount struct {
		success           int
		failure           int
		meaningfulFailure int
	}

	// result holds the result for a stress test.
	result struct {
		selects, inserts, deletes queryCount
	}
)

func (qc queryCount) successQPS(seconds float64) int {
	if seconds <= 0 {
		return 0
	}
	return int(float64(qc.success) / seconds)
}

func (qc queryCount) failureQPS(seconds float64) int {
	if seconds <= 0 {
		return 0
	}
	return int(float64(qc.failure) / seconds)
}

func (qc queryCount) meaningfulFailureQPS(seconds float64) int {
	if seconds <= 0 {
		return 0
	}
	return int(float64(qc.meaningfulFailure) / seconds)
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
		qc.meaningfulFailure += qci.meaningfulFailure
	}
	return qc
}

func (r result) assert() bool {
	return r.selects.meaningfulFailure == 0 && r.deletes.meaningfulFailure == 0 && r.inserts.meaningfulFailure == 0
}

// print renders the results held by result.
func (r result) print(log func(format string, args ...interface{}), seconds float64) {
	allQCs := sumQueryCounts(r.selects, r.inserts, r.deletes)
	log(`QPS:
	select: %d | failed: %d (including %d meaningful failures) | sum: %d
	insert: %d | failed: %d (including %d meaningful failures) | sum: %d
	delete: %d | failed: %d (including %d meaningful failures) | sum: %d
	---------
	total:	%d | failed: %d (including %d meaningful failures) | sum: %d

Queries:
	select: %d | failed: %d (including %d meaningful failures) | sum: %d
	insert: %d | failed: %d (including %d meaningful failures) | sum: %d
	delete: %d | failed: %d (including %d meaningful failures) | sum: %d
	---------
	total:	%d | failed: %d (including %d meaningful failures) | sum: %d
	
`, r.selects.successQPS(seconds), r.selects.failureQPS(seconds), r.selects.meaningfulFailureQPS(seconds), r.selects.totalQPS(seconds),
		r.inserts.successQPS(seconds), r.inserts.failureQPS(seconds), r.inserts.meaningfulFailureQPS(seconds), r.inserts.totalQPS(seconds),
		r.deletes.successQPS(seconds), r.deletes.failureQPS(seconds), r.deletes.meaningfulFailureQPS(seconds), r.deletes.totalQPS(seconds),
		allQCs.successQPS(seconds), allQCs.failureQPS(seconds), allQCs.meaningfulFailureQPS(seconds), allQCs.totalQPS(seconds),
		r.selects.success, r.selects.failure, r.selects.meaningfulFailure, r.selects.sum(),
		r.inserts.success, r.inserts.failure, r.inserts.meaningfulFailure, r.inserts.sum(),
		r.deletes.success, r.deletes.failure, r.deletes.meaningfulFailure, r.deletes.sum(),
		allQCs.success, allQCs.failure, allQCs.meaningfulFailure, allQCs.sum())
}
