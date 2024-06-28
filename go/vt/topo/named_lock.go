/*
Copyright 2024 The Vitess Authors.

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
	"context"
	"path"
)

type namedLock struct {
	name string
}

var _ iTopoLock = (*namedLock)(nil)

func (s *namedLock) Type() string {
	return "named"
}

func (s *namedLock) ResourceName() string {
	return s.name
}

func (s *namedLock) Path() string {
	return path.Join(NamedLocksPath, s.name)
}

// LockName will lock the opaque identifier, and return:
// - a context with a locksInfo structure for future reference.
// - an unlock method
// - an error if anything failed.
func (ts *Server) LockName(ctx context.Context, name, action string) (context.Context, func(*error), error) {
	return ts.internalLock(ctx, &namedLock{
		name: name,
	}, action, WithType(Named))
}

// CheckNameLocked can be called on a context to make sure we have the lock
// for a given opaque identifier.
func CheckNameLocked(ctx context.Context, name string) error {
	return checkLocked(ctx, &namedLock{
		name: name,
	})
}
