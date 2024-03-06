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

package collations

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllCollationsByCharset(t *testing.T) {
	var defaults1 = map[string][2]string{
		"utf8mb4": {"utf8mb4_general_ci", "utf8mb4_bin"},
	}
	var defaults2 = map[string][2]string{
		"utf8mb4": {"utf8mb4_0900_ai_ci", "utf8mb4_0900_bin"},
	}

	for _, tc := range []struct {
		version  collver
		defaults map[string][2]string
	}{
		{collverMariaDB100, defaults1},
		{collverMariaDB101, defaults1},
		{collverMariaDB102, defaults1},
		{collverMariaDB103, defaults1},
		{collverMySQL56, defaults1},
		{collverMySQL57, defaults1},
		{collverMySQL8, defaults2},
	} {
		t.Run(tc.version.String(), func(t *testing.T) {
			env := makeEnv(tc.version)
			for csname, cset := range env.byCharset {
				switch csname {
				case "gb18030":
					// this doesn't work yet
					continue
				}
				require.NotNil(t, cset.Default, "charset %s has no default", csname)
				require.NotNil(t, cset.Binary, "charset %s has no binary", csname)

			}

			for charset, expected := range tc.defaults {
				expectedDefault, expectedBinary := expected[0], expected[1]
				if def := env.DefaultCollationForCharset(charset); env.LookupName(def) != expectedDefault {
					t.Fatalf("bad default for utf8mb4: %s (expected %s)", env.LookupName(def), expectedDefault)
				}
				if def := env.BinaryCollationForCharset(charset); env.LookupName(def) != expectedBinary {
					t.Fatalf("bad binary for utf8mb4: %s (expected %s)", env.LookupName(def), expectedBinary)
				}
			}
		})
	}
}

// XTestSupportTables should not run by default; it is used to generate a Markdown
// table with Collation support information for the current build of Vitess.
func XTestSupportTables(t *testing.T) {
	var versions = []collver{
		collverMySQL8,
		collverMySQL57,
		collverMySQL56,
		collverMariaDB103,
		collverMariaDB102,
		collverMariaDB101,
		collverMariaDB100,
	}

	var envs []*Environment
	for _, v := range versions {
		envs = append(envs, makeEnv(v))
	}

	var all []ID
	for id := range globalVersionInfo {
		all = append(all, id)
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i] < all[j]
	})

	var out = os.Stdout

	fmt.Fprintf(out, "| Collation | Charset")
	for _, env := range envs {
		fmt.Fprintf(out, " | %s", env.version.String())
	}
	fmt.Fprintf(out, " |\n|%s\n", strings.Repeat("---|", len(envs)+2))

	for _, id := range all {
		name := envs[0].LookupName(id)
		if name == "" {
			vdata := globalVersionInfo[id]

			var collnames []string
			for _, alias := range vdata.alias {
				collnames = append(collnames, alias.name)
				break
			}
			collname := strings.Join(collnames, ",")
			parts := strings.Split(collname, "_")

			fmt.Fprintf(out, "| %s | %s", collname, parts[0])
			for _, env := range envs {
				var supported bool
				for _, alias := range vdata.alias {
					if alias.mask&env.version != 0 {
						supported = true
						break
					}
				}
				if supported {
					fmt.Fprintf(out, " | ⚠️")
				} else {
					fmt.Fprintf(out, " | ❌")
				}
			}
		} else {
			fmt.Fprintf(out, "| %s | %s", name, envs[0].LookupCharsetName(id))
			for _, env := range envs {
				_, supported := env.LookupID(name)
				if supported {
					fmt.Fprintf(out, " | ✅")
				} else {
					fmt.Fprintf(out, " | ❌")
				}
			}
		}

		fmt.Fprintf(out, " |\n")
	}
}
