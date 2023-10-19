/*
Copyright 2023 The Vitess Authors.

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

package sqlparser

import (
	"strconv"
	"strings"
)

// ReservedVars keeps track of the bind variable names that have already been used
// in a parsed query.
type ReservedVars struct {
	prefix       string
	reserved     BindVars
	next         []byte
	counter      int
	fast, static bool
	sqNext       int64
}

var subQueryBaseArgName = []byte("__sq")

// ReserveAll tries to reserve all the given variable names. If they're all available,
// they are reserved and the function returns true. Otherwise, the function returns false.
func (r *ReservedVars) ReserveAll(names ...string) bool {
	for _, name := range names {
		if _, ok := r.reserved[name]; ok {
			return false
		}
	}
	for _, name := range names {
		r.reserved[name] = struct{}{}
	}
	return true
}

// ReserveColName reserves a variable name for the given column; if a variable
// with the same name already exists, it'll be suffixed with a numberic identifier
// to make it unique.
func (r *ReservedVars) ReserveColName(col *ColName) string {
	reserveName := col.CompliantName()
	if r.fast && strings.HasPrefix(reserveName, r.prefix) {
		reserveName = "_" + reserveName
	}

	return r.ReserveVariable(reserveName)
}

func (r *ReservedVars) ReserveVariable(compliantName string) string {
	joinVar := []byte(compliantName)
	baseLen := len(joinVar)
	i := int64(1)

	for {
		if _, ok := r.reserved[string(joinVar)]; !ok {
			bvar := string(joinVar)
			r.reserved[bvar] = struct{}{}
			return bvar
		}
		joinVar = strconv.AppendInt(joinVar[:baseLen], i, 10)
		i++
	}
}

// ReserveSubQuery returns the next argument name to replace subquery with pullout value.
func (r *ReservedVars) ReserveSubQuery() string {
	for {
		r.sqNext++
		joinVar := strconv.AppendInt(subQueryBaseArgName, r.sqNext, 10)
		if _, ok := r.reserved[string(joinVar)]; !ok {
			r.reserved[string(joinVar)] = struct{}{}
			return string(joinVar)
		}
	}
}

// ReserveSubQueryWithHasValues returns the next argument name to replace subquery with pullout value.
func (r *ReservedVars) ReserveSubQueryWithHasValues() (string, string) {
	for {
		r.sqNext++
		joinVar := strconv.AppendInt(subQueryBaseArgName, r.sqNext, 10)
		hasValuesJoinVar := strconv.AppendInt(HasValueSubQueryBaseName, r.sqNext, 10)
		_, joinVarOK := r.reserved[string(joinVar)]
		_, hasValuesJoinVarOK := r.reserved[string(hasValuesJoinVar)]
		if !joinVarOK && !hasValuesJoinVarOK {
			r.reserved[string(joinVar)] = struct{}{}
			r.reserved[string(hasValuesJoinVar)] = struct{}{}
			return string(joinVar), string(hasValuesJoinVar)
		}
	}
}

// ReserveHasValuesSubQuery returns the next argument name to replace subquery with has value.
func (r *ReservedVars) ReserveHasValuesSubQuery() string {
	for {
		r.sqNext++
		joinVar := strconv.AppendInt(HasValueSubQueryBaseName, r.sqNext, 10)
		if _, ok := r.reserved[string(joinVar)]; !ok {
			bvar := string(joinVar)
			r.reserved[bvar] = struct{}{}
			return bvar
		}
	}
}

const staticBvar10 = "vtg0vtg1vtg2vtg3vtg4vtg5vtg6vtg7vtg8vtg9"
const staticBvar100 = "vtg10vtg11vtg12vtg13vtg14vtg15vtg16vtg17vtg18vtg19vtg20vtg21vtg22vtg23vtg24vtg25vtg26vtg27vtg28vtg29vtg30vtg31vtg32vtg33vtg34vtg35vtg36vtg37vtg38vtg39vtg40vtg41vtg42vtg43vtg44vtg45vtg46vtg47vtg48vtg49vtg50vtg51vtg52vtg53vtg54vtg55vtg56vtg57vtg58vtg59vtg60vtg61vtg62vtg63vtg64vtg65vtg66vtg67vtg68vtg69vtg70vtg71vtg72vtg73vtg74vtg75vtg76vtg77vtg78vtg79vtg80vtg81vtg82vtg83vtg84vtg85vtg86vtg87vtg88vtg89vtg90vtg91vtg92vtg93vtg94vtg95vtg96vtg97vtg98vtg99"

func (r *ReservedVars) nextUnusedVar() string {
	if r.fast {
		r.counter++

		if r.static {
			switch {
			case r.counter < 10:
				ofs := r.counter * 4
				return staticBvar10[ofs : ofs+4]
			case r.counter < 100:
				ofs := (r.counter - 10) * 5
				return staticBvar100[ofs : ofs+5]
			}
		}

		r.next = strconv.AppendInt(r.next[:len(r.prefix)], int64(r.counter), 10)
		return string(r.next)
	}

	for {
		r.counter++
		r.next = strconv.AppendInt(r.next[:len(r.prefix)], int64(r.counter), 10)
		if _, ok := r.reserved[string(r.next)]; !ok {
			bvar := string(r.next)
			r.reserved[bvar] = struct{}{}
			return bvar
		}
	}
}

// NewReservedVars allocates a ReservedVar instance that will generate unique
// variable names starting with the given `prefix` and making sure that they
// don't conflict with the given set of `known` variables.
func NewReservedVars(prefix string, known BindVars) *ReservedVars {
	rv := &ReservedVars{
		prefix:   prefix,
		counter:  0,
		reserved: known,
		fast:     true,
		next:     []byte(prefix),
	}

	if prefix != "" && prefix[0] == '_' {
		panic("cannot reserve variables with a '_' prefix")
	}

	for bvar := range known {
		if strings.HasPrefix(bvar, prefix) {
			rv.fast = false
			break
		}
	}

	if prefix == "vtg" {
		rv.static = true
	}
	return rv
}
