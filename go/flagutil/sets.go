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

package flagutil

import (
	"strings"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/sets"
)

var _ pflag.Value = (*StringSetFlag)(nil)

// StringSetFlag can be used to collect multiple instances of a flag into a set
// of values.
//
// For example, defining the following:
//
//	var x flagutil.StringSetFlag
//	flag.Var(&x, "foo", "")
//
// And then specifying "-foo x -foo y -foo x", will result in a set of {x, y}.
//
// In addition to implemnting the standard flag.Value interface, it also
// provides an implementation of pflag.Value, so it is usable in libraries like
// cobra.
type StringSetFlag struct {
	set sets.Set[string]
}

// ToSet returns the underlying string set, or an empty set if the underlying
// set is nil.
func (set *StringSetFlag) ToSet() sets.Set[string] {
	if set.set == nil {
		set.set = sets.New[string]()
	}

	return set.set
}

// Set is part of the pflag.Value and flag.Value interfaces.
func (set *StringSetFlag) Set(s string) error {
	if set.set == nil {
		set.set = sets.New[string]()
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

	return strings.Join(sets.List(set.set), ", ")
}

// Type is part of the pflag.Value interface.
func (set *StringSetFlag) Type() string { return "StringSetFlag" }
