package tests

import (
	"testing"
)

// Spec is an access point to test Expections
type Spec struct {
	t *testing.T
}

// S generates a spec. You will want to use it once in a test file, once in a test or once per each check
func S(t *testing.T) *Spec {
	return &Spec{t: t}
}

// ExpectNil expects given value to be nil, or errors
func (spec *Spec) ExpectNil(actual interface{}) {
	if actual == nil {
		return
	}
	spec.t.Errorf("Expected %+v to be nil", actual)
}

// ExpectNotNil expects given value to be not nil, or errors
func (spec *Spec) ExpectNotNil(actual interface{}) {
	if actual != nil {
		return
	}
	spec.t.Errorf("Expected %+v to be not nil", actual)
}

// ExpectEquals expects given values to be equal (comparison via `==`), or errors
func (spec *Spec) ExpectEquals(actual, value interface{}) {
	if actual == value {
		return
	}
	spec.t.Errorf("Expected:\n[[[%+v]]]\n- got:\n[[[%+v]]]", value, actual)
}

// ExpectNotEquals expects given values to be nonequal (comparison via `==`), or errors
func (spec *Spec) ExpectNotEquals(actual, value interface{}) {
	if !(actual == value) {
		return
	}
	spec.t.Errorf("Expected not %+v", value)
}

// ExpectEqualsAny expects given actual to equal (comparison via `==`) at least one of given values, or errors
func (spec *Spec) ExpectEqualsAny(actual interface{}, values ...interface{}) {
	for _, value := range values {
		if actual == value {
			return
		}
	}
	spec.t.Errorf("Expected %+v to equal any of given values", actual)
}

// ExpectNotEqualsAny expects given actual to be nonequal (comparison via `==`)tp any of given values, or errors
func (spec *Spec) ExpectNotEqualsAny(actual interface{}, values ...interface{}) {
	for _, value := range values {
		if actual == value {
			spec.t.Errorf("Expected not %+v", value)
		}
	}
}

// ExpectFalse expects given values to be false, or errors
func (spec *Spec) ExpectFalse(actual interface{}) {
	spec.ExpectEquals(actual, false)
}

// ExpectTrue expects given values to be true, or errors
func (spec *Spec) ExpectTrue(actual interface{}) {
	spec.ExpectEquals(actual, true)
}
