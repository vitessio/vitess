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
	dependencies interface {
		Empty() bool
		Get() (dependency, error)
		Merge(dependencies) (dependencies, error)
	}
	dependency struct {
		direct    TableSet
		recursive TableSet
		typ       *querypb.Type
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

var _ dependencies = (*nothing)(nil)
var _ dependencies = (*certain)(nil)
var _ dependencies = (*uncertain)(nil)

func (u *uncertain) Empty() bool {
	return false
}

func (u *uncertain) Get() (dependency, error) {
	if u.fail {
		return dependency{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "ambiguous")
	}
	return u.dependency, nil
}

func (u *uncertain) Merge(d dependencies) (dependencies, error) {
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
	return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "ambiguous")
}

func (c *certain) Empty() bool {
	return false
}

func (c *certain) Get() (dependency, error) {
	return c.dependency, nil
}

func (c *certain) Merge(d dependencies) (dependencies, error) {
	switch d := d.(type) {
	case *nothing, *uncertain:
		return c, nil
	case *certain:
		if d.recursive == c.recursive {
			return c, nil
		}
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "ambiguous")
}

func (n *nothing) Empty() bool {
	return true
}

func (n *nothing) Get() (dependency, error) {
	return dependency{}, nil
}

func (n *nothing) Merge(d dependencies) (dependencies, error) {
	return d, nil
}
