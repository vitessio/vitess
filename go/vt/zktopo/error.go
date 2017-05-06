/*
Copyright 2017 Google Inc.

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

package zktopo

import (
	zookeeper "github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// Error codes returned by the zookeeper Go client:
func convertError(err error) error {
	switch err {
	case zookeeper.ErrBadVersion:
		return topo.ErrBadVersion
	case zookeeper.ErrNoNode:
		return topo.ErrNoNode
	case zookeeper.ErrNodeExists:
		return topo.ErrNodeExists
	case zookeeper.ErrNotEmpty:
		return topo.ErrNotEmpty
	case zookeeper.ErrSessionExpired:
		return topo.ErrTimeout
	case context.Canceled:
		return topo.ErrInterrupted
	case context.DeadlineExceeded:
		return topo.ErrTimeout
	}
	return err
}
