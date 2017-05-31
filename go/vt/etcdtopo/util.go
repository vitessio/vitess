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

package etcdtopo

import (
	"path"

	"github.com/coreos/go-etcd/etcd"
)

// getNodeNames returns a list of sub-node names listed in the given Response.
// Key names are given as fully qualified paths in the Response, so we return
// the base name.
func getNodeNames(resp *etcd.Response) ([]string, error) {
	if resp.Node == nil {
		return nil, ErrBadResponse
	}

	names := make([]string, 0, len(resp.Node.Nodes))
	for _, n := range resp.Node.Nodes {
		names = append(names, path.Base(n.Key))
	}
	return names, nil
}
