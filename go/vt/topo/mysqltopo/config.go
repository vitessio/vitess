/*
Copyright 2025 The Vitess Authors.

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

// Package mysqltopo implements topo.Server with MySQL as the backend.
package mysqltopo

import (
	"flag"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
)

const (
	// DefaultTopoSchema is the default schema name for the topo data
	DefaultTopoSchema = "topo"
)

var (
	mysqlTopoLockTTL = flag.Duration("topo-mysql-lock-ttl", 5*time.Minute, "TTL for MySQL topo locks")
)

func init() {
	servenv.OnParseFor("vtctld", registerFlags)
	servenv.OnParseFor("vttablet", registerFlags)
	servenv.OnParseFor("vtgate", registerFlags)
	servenv.OnParseFor("vtorc", registerFlags)
}

func registerFlags(fs *pflag.FlagSet) {
	pflag.CommandLine.AddGoFlag(flag.Lookup("topo-mysql-lock-ttl"))
}
