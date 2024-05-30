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
	"github.com/spf13/cobra"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cmd/zk/internal/zkfs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
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

The zk tool looks for the address of the cluster in /etc/zookeeper/zk_client.conf,
or the file specified in the ZK_CLIENT_CONFIG environment variable.

The local cell may be overridden with the ZK_CLIENT_LOCAL_CELL environment
variable.`,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			logutil.PurgeLogs()

			// Connect to the server.
			fs = &zkfs.FS{
				Conn: zk2topo.Connect(server),
			}
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			logutil.Flush()
		},
	}
)

func init() {
	Root.Flags().StringVar(&server, "server", server, "server(s) to connect to")

	log.RegisterFlags(Root.Flags())
	logutil.RegisterFlags(Root.Flags())
	acl.RegisterFlags(Root.Flags())
}
