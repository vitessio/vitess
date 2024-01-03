/*
Copyright 2019 The Vitess Authors.

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

package zk2topo

import (
	"context"
	"errors"

	"github.com/z-division/go-zookeeper/zk"

	"vitess.io/vitess/go/vt/topo"
)

// Error codes returned by the zookeeper Go client:
func convertError(err error, node string) error {
	switch {
	case errors.Is(err, zk.ErrBadVersion):
		return topo.NewError(topo.BadVersion, node)
	case errors.Is(err, zk.ErrNoNode):
		return topo.NewError(topo.NoNode, node)
	case errors.Is(err, zk.ErrNodeExists):
		return topo.NewError(topo.NodeExists, node)
	case errors.Is(err, zk.ErrNotEmpty):
		return topo.NewError(topo.NodeNotEmpty, node)
	case errors.Is(err, zk.ErrSessionExpired):
		return topo.NewError(topo.Timeout, node)
	case errors.Is(err, context.Canceled):
		return topo.NewError(topo.Interrupted, node)
	case errors.Is(err, context.DeadlineExceeded):
		return topo.NewError(topo.Timeout, node)
	}
	return err
}
