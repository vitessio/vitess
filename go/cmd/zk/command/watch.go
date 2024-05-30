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
	"fmt"

	"github.com/spf13/cobra"
	"github.com/z-division/go-zookeeper/zk"

	"vitess.io/vitess/go/cmd/zk/internal/zkfilepath"
	"vitess.io/vitess/go/vt/log"
)

var Watch = &cobra.Command{
	Use:     "watch <path>",
	Short:   "Watches for changes to nodes and prints events as they occur.",
	Example: `watch /zk/path`,
	Args:    cobra.MinimumNArgs(1),
	RunE:    commandWatch,
}

func commandWatch(cmd *cobra.Command, args []string) error {
	eventChan := make(chan zk.Event, 16)
	for _, arg := range cmd.Flags().Args() {
		zkPath := zkfilepath.Clean(arg)
		_, _, watch, err := fs.Conn.GetW(cmd.Context(), zkPath)
		if err != nil {
			return fmt.Errorf("watch error: %v", err)
		}
		go func() {
			eventChan <- <-watch
		}()
	}

	for {
		select {
		case <-cmd.Context().Done():
			return nil
		case event := <-eventChan:
			log.Infof("watch: event %v: %v", event.Path, event)
			if event.Type == zk.EventNodeDataChanged {
				data, stat, watch, err := fs.Conn.GetW(cmd.Context(), event.Path)
				if err != nil {
					return fmt.Errorf("ERROR: failed to watch %v", err)
				}
				log.Infof("watch: %v %v\n", event.Path, stat)
				println(data)
				go func() {
					eventChan <- <-watch
				}()
			} else if event.State == zk.StateDisconnected {
				return nil
			} else if event.Type == zk.EventNodeDeleted {
				log.Infof("watch: %v deleted\n", event.Path)
			} else {
				// Most likely a session event - try t
				_, _, watch, err := fs.Conn.GetW(cmd.Context(), event.Path)
				if err != nil {
					return fmt.Errorf("ERROR: failed to watch %v", err)
				}
				go func() {
					eventChan <- <-watch
				}()
			}
		}
	}
}

func init() {
	Root.AddCommand(Watch)
}
