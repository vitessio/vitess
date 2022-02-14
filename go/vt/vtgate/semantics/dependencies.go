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
		merge(dependencies) (dependencies, error)
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

func (u *uncertain) merge(d dependencies) (dependencies, error) {
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

func (c *certain) merge(d dependencies) (dependencies, error) {
	switch d := d.(type) {
	case *nothing, *uncertain:
		return c, nil
	case *certain:
		if d.recursive == c.recursive {
			return c, nil
		}
	}

	return nil, ambigousErr
}

func (n *nothing) empty() bool {
	return true
}

func (n *nothing) get() (dependency, error) {
	return dependency{}, nil
}

func (n *nothing) merge(d dependencies) (dependencies, error) {
	return d, nil
}
