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

/*
 Detect server flavors and capabilities
*/

package mysqlctl

type serverVersion struct {
	Major, Minor, Patch int
}

func (v *serverVersion) atLeast(compare serverVersion) bool {
	if v.Major > compare.Major {
		return true
	}
	if v.Major == compare.Major && v.Minor > compare.Minor {
		return true
	}
	if v.Major == compare.Major && v.Minor == compare.Minor && v.Patch >= compare.Patch {
		return true
	}
	return false
}
