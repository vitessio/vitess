/*
Copyright 2019 The Vitess Authors.

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

package planbuilder

import (
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// logicalPlan defines the interface that a primitive must
// satisfy.
type logicalPlan interface {
	// Primitive returns the underlying primitive.
	Primitive() engine.Primitive
}

// -------------------------------------------------------------------------

// logicalPlanCommon implements some common functionality of builders.
// Make sure to override in case behavior needs to be changed.
type logicalPlanCommon struct {
	order int
	input logicalPlan
}

func newBuilderCommon(input logicalPlan) logicalPlanCommon {
	return logicalPlanCommon{input: input}
}

func (bc *logicalPlanCommon) Order() int {
	return bc.order
}

// -------------------------------------------------------------------------

// resultsBuilder is a superset of logicalPlanCommon. It also handles
// resultsColumn functionality.
type resultsBuilder struct {
	logicalPlanCommon
	truncater truncater
}

func newResultsBuilder(input logicalPlan, truncater truncater) resultsBuilder {
	return resultsBuilder{
		logicalPlanCommon: newBuilderCommon(input),
		truncater:         truncater,
	}
}
