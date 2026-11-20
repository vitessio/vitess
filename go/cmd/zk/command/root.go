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

package command

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cmd/zk/internal/zkfs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo/zk2topo"
)

var (
	fs     *zkfs.FS
	server string

	Root = &cobra.Command{
		Use:   "zk",
		Short: "zk is a tool for wrangling zookeeper.",
		Long: `zk is a tool for wrangling zookeeper.

It tries to mimic unix file system commands wherever possible, but
there are some slight differences in flag handling.

Pass the address of the cluster with --server, for example
zk --server localhost:2181 ls /`,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := log.Init(cmd.Flags()); err != nil {
				return err
			}

			logutil.PurgeLogs()

			// Connect to the server.
			fs = &zkfs.FS{
				Conn: zk2topo.Connect(server),
			}

			return nil
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			logutil.Flush()
		},
	}
)

func init() {
	// These are persistent flags so that every subcommand accepts them, e.g.
	// `zk --server localhost:2181 ls /`.
	fs := Root.PersistentFlags()
	fs.StringVar(&server, "server", server, "server(s) to connect to")

	log.RegisterFlags(fs)
	logutil.RegisterFlags(fs)
	acl.RegisterFlags(fs)

	// zk2topo registers its connection flags (--topo-zk-tls-*,
	// --topo-zk-auth-file, ...) for "zk" through servenv. Only take those:
	// the other servenv flags (config files, pprof, --version) are not
	// wired up in this tool.
	servenv.GetFlagSetFor("zk").VisitAll(func(f *pflag.Flag) {
		if strings.HasPrefix(f.Name, "topo-zk-") {
			fs.AddFlag(f)
		}
	})
}
