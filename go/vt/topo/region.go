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

package topo

import (
	"golang.org/x/net/context"
)

type regionMap struct {
	cellsToRegions map[string]string
}

var regions = regionMap{
	cellsToRegions: make(map[string]string),
}

// InitRegions resolves the region name for the given set of
// cells. It must be called during initialization time before
// GetRegion is ever used.
//
// By convention, if there is no region specified in the cell info,
// we use the cell name itself as the region.
func InitRegions(ctx context.Context, ts *Server, cells []string) error {
	for _, cell := range cells {
		info, err := ts.GetCellInfo(ctx, cell, false)
		switch {
		case err == nil:
			if info.Region != "" {
				regions.cellsToRegions[cell] = info.Region
			} else {
				regions.cellsToRegions[cell] = cell
			}
		case IsErrType(err, NoNode):
			regions.cellsToRegions[cell] = cell
		default:
			return err
		}
	}

	return nil
}

// SetRegionMap is used only for tests.
func SetRegionMap(cellsToRegions map[string]string) {
	regions.cellsToRegions = cellsToRegions
}

// GetRegion returns the region for the given `cell`.
//
// By convention, if there is no region specified in the cell info,
// we use the cell name itself as the region.
//
// Note that the access to regions is *NOT* protected by a
// mutex. All region mappings must be defined statically at
// initialization time so this mapping is safe to access
// concurrently.
func GetRegion(cell string) string {
	if region, ok := regions.cellsToRegions[cell]; ok {
		return region
	}
	return cell
}
