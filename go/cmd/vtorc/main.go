/*
   Copyright 2014 Outbrain Inc.

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
	"vitess.io/vitess/go/cmd/vtorc/cli"
	"vitess.io/vitess/go/vt/log"
)

// main is the application's entry point. It will spawn an HTTP interface.
func main() {
	// TODO: viperutil.BindFlags()

	if err := cli.Main.Execute(); err != nil {
		log.Exit(err)
	}
}
