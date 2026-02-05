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

package funcs

import (
	"strings"

	"github.com/spf13/viper"
)

// GetPath returns a GetFunc that expands a slice of strings into individual
// paths based on standard POSIX shell $PATH separator parsing.
func GetPath(v *viper.Viper) func(key string) []string {
	return func(key string) (paths []string) {
		for _, val := range v.GetStringSlice(key) {
			if val != "" {
				paths = append(paths, strings.Split(val, ":")...)
			}
		}

		return paths
	}
}
