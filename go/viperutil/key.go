/*
Copyright 2022 The Vitess Authors.

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

package viperutil

import "strings"

// Key returns a full key given a prefix and optional key. If key is the empty
// string, only the prefix is returned, otherwise the prefix is joined with the
// key with ".", the default viper key delimiter.
func Key(prefix string, key string) string {
	parts := []string{prefix}
	if key != "" {
		parts = append(parts, key)
	}

	return strings.Join(parts, ".")
}

// KeyPartial returns a partial func that returns full keys joined to the given
// prefix.
//
// Packages can use this to avoid repeating a prefix across all their key
// accesses, for example:
//
//	package mypkg
//	var (
//		configKey = viperutil.KeyPartial("foo.bar.mypkg")
//		v1 = viperutil.NewValue(configKey("v1"), ...) // bound to "foo.bar.mypkg.v1"
//		v2 = viperutil.NewValue(configKey("v2"), ...) // bound to "foo.bar.mypkg.v2"
//		...
//	)
func KeyPartial(prefix string) func(string) string {
	return func(key string) string {
		return Key(prefix, key)
	}
}
