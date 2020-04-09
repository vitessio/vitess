/*
Copyright 2020 The Vitess Authors.

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

package k8stopo

import (
	"context"

	"k8s.io/apimachinery/pkg/api/errors"

	"vitess.io/vitess/go/vt/topo"
)

// convertError converts errors into a topo error. All errors
// are either application-level errors, or context errors.
func convertError(err error, nodePath string) error {
	if err == nil {
		return nil
	}

	// Check for specific kubernetes errors
	if errors.IsAlreadyExists(err) {
		return topo.NewError(topo.NodeExists, nodePath)
	}
	if errors.IsNotFound(err) {
		return topo.NewError(topo.NoNode, nodePath)
	}
	if errors.IsServerTimeout(err) {
		return topo.NewError(topo.Timeout, nodePath)
	}

	// Convert specific context sentinel values.
	switch err {
	case context.Canceled:
		return topo.NewError(topo.Interrupted, nodePath)
	case context.DeadlineExceeded:
		return topo.NewError(topo.Timeout, nodePath)
	}

	return err
}
