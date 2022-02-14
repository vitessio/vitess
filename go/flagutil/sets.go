package flagutil

import (
	"flag"
	"strings"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/util/sets"
)

var (
	_ flag.Value  = (*StringSetFlag)(nil)
	_ pflag.Value = (*StringSetFlag)(nil)
)

// StringSetFlag can be used to collect multiple instances of a flag into a set
// of values.
//
// For example, defining the following:
//
//		var x flagutil.StringSetFlag
//		flag.Var(&x, "foo", "")
//
// And then specifying "-foo x -foo y -foo x", will result in a set of {x, y}.
//
// In addition to implemnting the standard flag.Value interface, it also
// provides an implementation of pflag.Value, so it is usable in libraries like
// cobra.
type StringSetFlag struct {
	set sets.String
}

// ToSet returns the underlying string set, or an empty set if the underlying
// set is nil.
func (set *StringSetFlag) ToSet() sets.String {
	if set.set == nil {
		set.set = sets.NewString()
	}

	return set.set
}

// Set is part of the pflag.Value and flag.Value interfaces.
func (set *StringSetFlag) Set(s string) error {
	if set.set == nil {
		set.set = sets.NewString(s)
		return nil
	}

	set.set.Insert(s)
	return nil
}

// String is part of the pflag.Value and flag.Value interfaces.
func (set *StringSetFlag) String() string {
	if set.set == nil {
		return ""
	}

	return strings.Join(set.set.List(), ", ")
}

// Type is part of the pflag.Value interface.
func (set *StringSetFlag) Type() string { return "StringSetFlag" }
