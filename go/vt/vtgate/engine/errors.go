package engine

import (
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/stats"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vterrors"
)

type ErrorMode int

const (
	ErrorModeAggregate = ErrorMode(iota)
	ErrorModeAsWarningsIfResultElseAggregate
	ErrorModeAsWarnings
)

type errorContext struct {
	errs      []error
	numShards int
	session   SessionActions
}

var (
	partialSuccessScatterQueries = stats.NewCounter("PartialSuccessScatterQueries", "Count of partially successful scatter queries")
)

func (em ErrorMode) CanRecordWarnings() bool {
	return em != ErrorModeAggregate
}

func errorFuncAggregate(c *errorContext) error {
	if len(c.errs) == 0 {
		return nil
	}

	return vterrors.Aggregate(c.errs)
}

func errorFuncAsWarnings(c *errorContext) error {
	for _, err := range c.errs {
		serr := mysql.NewSQLErrorFromError(err).(*mysql.SQLError)
		c.session.RecordWarning(
			&querypb.QueryWarning{Code: uint32(serr.Num), Message: err.Error()},
		)
	}

	if len(c.errs) < c.numShards {
		partialSuccessScatterQueries.Add(1)
	}

	return nil
}

func errorFuncAsWarningsIfResultElseAggregate(c *errorContext) error {
	if len(c.errs) < c.numShards {
		return errorFuncAsWarnings(c)
	}

	return errorFuncAggregate(c)
}

func filterOutNilErrors(errs []error) []error {
	var errors []error
	for _, err := range errs {
		if err != nil {
			errors = append(errors, err)
		}
	}
	return errors
}

func newErrorContext(errs []error, numShards int, session SessionActions) *errorContext {
	return &errorContext{
		errs:      filterOutNilErrors(errs),
		numShards: numShards,
		session:   session,
	}
}

func (ec *errorContext) apply(mode ErrorMode) error {
	switch mode {
	case ErrorModeAsWarnings:
		return errorFuncAsWarnings(ec)
	case ErrorModeAsWarningsIfResultElseAggregate:
		return errorFuncAsWarningsIfResultElseAggregate(ec)
	}
	return errorFuncAggregate(ec)
}
