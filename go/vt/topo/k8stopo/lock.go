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
	"time"

	"context"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"vitess.io/vitess/go/vt/topo"
	vtv1beta1 "vitess.io/vitess/go/vt/topo/k8stopo/apis/topo/v1beta1"
)

// kubernetesLockDescriptor implements topo.LockDescriptor.
type kubernetesLockDescriptor struct {
	s         *Server
	leaseID   string
	leasePath string
}

// Lock is part of the topo.Conn interface.
func (s *Server) Lock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	return s.lock(ctx, dirPath, contents, false)
}

// lock is used by both Lock() and master election.
// it blocks until the lock is taken, interrupted, or times out
func (s *Server) lock(ctx context.Context, nodePath, contents string, createMissing bool) (topo.LockDescriptor, error) {
	// Satisfy the topo.Conn interface
	if !createMissing {
		// Per the topo.Conn interface:
		// "Returns ErrNoNode if the directory doesn't exist (meaning
		//   there is no existing file under that directory)."
		if _, err := s.ListDir(ctx, nodePath, false); err != nil {
			return nil, convertError(err, nodePath)
		}
	}

	resource, err := s.buildFileResource(nodePath, []byte(contents))
	if err != nil {
		return nil, convertError(err, nodePath)
	}

	// mark locks as ephemeral
	resource.Data.Ephemeral = true

	var final *vtv1beta1.VitessTopoNode

	for {
		// Try and and create the resource. The kube api will handle the actual atomic lock creation
		final, err = s.resourceClient.Create(resource)
		if errors.IsAlreadyExists(err) {
			select {
			case <-time.After(10 * time.Millisecond):
				continue // retry
			case <-ctx.Done():
				return nil, convertError(ctx.Err(), nodePath)
			}
		} else if err != nil {
			return nil, convertError(err, nodePath)
		}

		break
	}

	// Update the internal cache
	err = s.memberIndexer.Update(final)
	if err != nil {
		return nil, convertError(err, nodePath)
	}

	return &kubernetesLockDescriptor{
		s:         s,
		leaseID:   resource.Name,
		leasePath: resource.Data.Key,
	}, nil
}

// Check is part of the topo.LockDescriptor interface.
func (ld *kubernetesLockDescriptor) Check(ctx context.Context) error {
	// Get the object and ensure the leaseid
	_, err := ld.s.resourceClient.Get(ld.leaseID, metav1.GetOptions{}) // TODO namespacing
	if err != nil {
		return convertError(err, ld.leasePath)

	}

	return nil
}

// Unlock is part of the topo.LockDescriptor interface.
func (ld *kubernetesLockDescriptor) Unlock(ctx context.Context) error {
	err := ld.s.resourceClient.Delete(ld.leaseID, &metav1.DeleteOptions{}) // TODO namespacing
	if err != nil {
		return convertError(err, ld.leasePath)
	}
	return nil
}
