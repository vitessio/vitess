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

package topo

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func (ts *Server) SaveWorkflowMetadata(ctx context.Context, targetKeyspace string, wm *topodata.WorkflowMetadata) (err error) {
	ctx, unlock, lockErr := ts.LockKeyspace(ctx, targetKeyspace, "GetWorkflowMetadata")
	if lockErr != nil {
		err = lockErr
		return err
	}
	defer unlock(&err)

	ki, err := ts.GetKeyspace(ctx, targetKeyspace)
	for _, w := range ki.Workflows {
		if w.Name == wm.Name {
			return vterrors.New(vtrpc.Code_ALREADY_EXISTS,
				fmt.Sprintf("workflow %s already exists in %s", w.Name, ki.KeyspaceName()))
		}
	}
	ki.Workflows = append(ki.Workflows, &topodata.WorkflowMetadata{
		Name:           wm.Name,
		Type:           wm.Type,
		TargetShards:   wm.TargetShards,
		SourceKeyspace: wm.SourceKeyspace,
		SourceShards:   wm.SourceShards,
	})
	if err := ts.UpdateKeyspace(ctx, ki); err != nil {
		return err
	}
	return nil
}

func (ts *Server) DeleteWorkflowMetadata(ctx context.Context, targetKeyspace, workflowName string) (err error) {
	ctx, unlock, lockErr := ts.LockKeyspace(ctx, targetKeyspace, "GetWorkflowMetadata")
	if lockErr != nil {
		err = lockErr
		return err
	}
	defer unlock(&err)

	ki, err := ts.GetKeyspace(ctx, targetKeyspace)
	if err != nil {
		return err
	}
	for i, w := range ki.Workflows {
		if w.Name == workflowName {
			ki.Workflows = append(ki.Workflows[:i], ki.Workflows[i+1:]...)
			if err := ts.UpdateKeyspace(ctx, ki); err != nil {
				return err
			}
			return nil
		}
	}
	return nil
}

func (ts *Server) GetWorkflowMetadata(ctx context.Context, targetKeyspace, workflowName string) (wm *topodata.WorkflowMetadata, err error) {
	ctx, unlock, lockErr := ts.LockKeyspace(ctx, targetKeyspace, "GetWorkflowMetadata")
	if lockErr != nil {
		err = lockErr
		return nil, err
	}
	defer unlock(&err)

	ki, err := ts.GetKeyspace(ctx, targetKeyspace)
	if err != nil {
		return nil, err
	}
	for _, w := range ki.Workflows {
		if w.Name == workflowName {
			return w, nil
		}
	}
	return nil, nil
}
