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
	"flag"

	_ "github.com/go-sql-driver/mysql"
	_ "modernc.org/sqlite"

	"vitess.io/vitess/go/cmd/vtorc/cli"
	_flag "vitess.io/vitess/go/internal/flag"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
)

// main is the application's entry point. It will spawn an HTTP interface.
func main() {
	servenv.RegisterDefaultFlags()
	servenv.RegisterFlags()

	cli.Main.Flags().AddFlagSet(servenv.GetFlagSetFor("vtorc"))

	// glog flags, no better way to do this
	_flag.PreventGlogVFlagFromClobberingVersionFlagShorthand(cli.Main.Flags())
	cli.Main.Flags().AddGoFlag(flag.Lookup("logtostderr"))
	cli.Main.Flags().AddGoFlag(flag.Lookup("alsologtostderr"))
	cli.Main.Flags().AddGoFlag(flag.Lookup("stderrthreshold"))
	cli.Main.Flags().AddGoFlag(flag.Lookup("log_dir"))

	// TODO: viperutil.BindFlags()

	if err := cli.Main.Execute(); err != nil {
		log.Exit(err)
	}
}
