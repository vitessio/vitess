// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

type PlanID int

const (
	NoPlan = PlanID(iota)
	SelectUnsharded
	SelectSinglePrimary
	SelectMultiPrimary
	SelectSingleLookup
	SelectMultiLookup
	SelectScatter
)

type Plan struct {
	ID     PlanID
	Reason string
	Query  string
	Index  *GateIndex
	Values []interface{}
}
