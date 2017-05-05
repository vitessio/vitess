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

package zk2topo

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// This file contains the cell management methods of zktopo.Server.
// Eventually this will be moved to the go/vt/topo package.

// GetKnownCells is part of the topo.Server interface.
func (zs *Server) GetKnownCells(ctx context.Context) ([]string, error) {
	return zs.ListDir(ctx, topo.GlobalCell, cellsPath)
}
