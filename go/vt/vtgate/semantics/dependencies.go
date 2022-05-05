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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// these types are used to go over dependencies provided by multiple
	// tables and figure out bindings and/or errors by merging dependencies together
	dependencies interface {
		empty() bool
		get() (dependency, error)
		merge(other dependencies, allowMulti bool) (dependencies, error)
	}
	dependency struct {
		direct    TableSet
		recursive TableSet
		typ       *Type
	}
	nothing struct{}
	certain struct {
		dependency
	}
	uncertain struct {
		dependency
		fail bool
	}
)

var ambigousErr = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "ambiguous")

func createCertain(direct TableSet, recursive TableSet, qt *Type) *certain {
	c := &certain{
		dependency: dependency{
			direct:    direct,
			recursive: recursive,
		},
	}
	if qt != nil && qt.Type != querypb.Type_NULL_TYPE {
		c.typ = qt
	}
	return c
}

func createUncertain(direct TableSet, recursive TableSet) *uncertain {
	return &uncertain{
		dependency: dependency{
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

func (u *uncertain) get() (dependency, error) {
	if u.fail {
		return dependency{}, ambigousErr
	}
	return u.dependency, nil
}

func (u *uncertain) merge(d dependencies, _ bool) (dependencies, error) {
	switch d := d.(type) {
	case *nothing:
		return u, nil
	case *uncertain:
		if d.recursive == u.recursive {
			return u, nil
		}
		u.fail = true
		return u, nil
	case *certain:
		return d, nil
	}
	return nil, ambigousErr
}

func (c *certain) empty() bool {
	return false
}

func (c *certain) get() (dependency, error) {
	return c.dependency, nil
}

func (c *certain) merge(d dependencies, allowMulti bool) (dependencies, error) {
	switch d := d.(type) {
	case *certain:
		if d.recursive == c.recursive {
			return c, nil
		}
		if !allowMulti {
			return nil, ambigousErr
		}
		c.direct.MergeInPlace(d.direct)
		c.recursive.MergeInPlace(d.recursive)
		return c, nil
	}

	return c, nil
}

func (n *nothing) empty() bool {
	return true
}

func (n *nothing) get() (dependency, error) {
	return dependency{}, nil
}

func (n *nothing) merge(d dependencies, _ bool) (dependencies, error) {
	return d, nil
}
