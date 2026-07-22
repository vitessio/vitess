/*
Copyright 2026 The Vitess Authors.

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

package main

import (
	"log"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cmd/rulesctl/cmd"
	vtlog "vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/utils"
)

func main() {
	rootCmd := cmd.Main()
	rootCmd.SetGlobalNormalizationFunc(utils.NormalizeUnderscoresToDashes)
	vtlog.RegisterFlags(rootCmd.PersistentFlags())
	acl.RegisterFlags(rootCmd.PersistentFlags())
	servenv.RegisterMySQLServerFlags(rootCmd.PersistentFlags())
	if err := rootCmd.Execute(); err != nil {
		log.Printf("%v", err)
	}
}
