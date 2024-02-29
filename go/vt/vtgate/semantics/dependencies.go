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

package semantics

import (
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type (
	// these types are used to go over dependencies provided by multiple
	// tables and figure out bindings and/or errors by merging dependencies together
	dependencies interface {
		empty() bool
		get(col *sqlparser.ColName) (dependency, error)
		merge(other dependencies, allowMulti bool) dependencies
	}
	dependency struct {
		certain   bool
		direct    TableSet
		recursive TableSet
		typ       evalengine.Type
	}
	nothing struct{}
	certain struct {
		dependency
		err bool
	}
	uncertain struct {
		dependency
		fail bool
	}
)

func createCertain(direct TableSet, recursive TableSet, qt evalengine.Type) *certain {
	c := &certain{
		dependency: dependency{
			certain:   true,
			direct:    direct,
			recursive: recursive,
		},
	}
	if qt.Valid() && qt.Type() != querypb.Type_NULL_TYPE {
		c.typ = qt
	}
	return c
}

func createUncertain(direct TableSet, recursive TableSet) *uncertain {
	return &uncertain{
		dependency: dependency{
			certain:   false,
			direct:    direct,
			recursive: recursive,
		},
	}
}

var _ dependencies = (*nothing)(nil)
var _ dependencies = (*certain)(nil)
var _ dependencies = (*uncertain)(nil)

func (u *uncertain) empty() bool {
	return false
}

func (u *uncertain) get(col *sqlparser.ColName) (dependency, error) {
	if u.fail {
		return dependency{}, newAmbiguousColumnError(col)
	}
	return u.dependency, nil
}

func (u *uncertain) merge(d dependencies, _ bool) dependencies {
	switch d := d.(type) {
	case *uncertain:
		if d.recursive != u.recursive {
			u.fail = true
		}
		return u
	case *certain:
		return d
	default:
		return u
	}
}

func (c *certain) empty() bool {
	return false
}

func (c *certain) get(col *sqlparser.ColName) (dependency, error) {
	if c.err {
		return c.dependency, newAmbiguousColumnError(col)
	}
	return c.dependency, nil
}

func (c *certain) merge(d dependencies, allowMulti bool) dependencies {
	switch d := d.(type) {
	case *certain:
		if d.recursive == c.recursive {
			return c
		}
		c.direct = c.direct.Merge(d.direct)
		c.recursive = c.recursive.Merge(d.recursive)
		if !allowMulti {
			c.err = true
		}

		return c
	}

	return c
}

func (n *nothing) empty() bool {
	return true
}

func (n *nothing) get(*sqlparser.ColName) (dependency, error) {
	return dependency{certain: true}, nil
}

func (n *nothing) merge(d dependencies, _ bool) dependencies {
	return d
}
