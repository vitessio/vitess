/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topovalidator

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// This file contains the UITest validator. It doesn't do anything,
// but create a Fixer with a couple actions, to test the UI.

// RegisterUITestValidator registers the UITest Validator.
func RegisterUITestValidator() {
	RegisterValidator("UITest Validator", &UITestValidator{})
}

// UITestValidator implements Validator.
type UITestValidator struct{}

// Audit is part of the Validator interface.
func (kv *UITestValidator) Audit(ctx context.Context, ts topo.Server, w *Workflow) error {
	w.AddFixer("UITest Fixer", "UI Test Fixer", &UITestFixer{}, []string{"Success", "Error"})
	return nil
}

// UITestFixer implements Fixer.
type UITestFixer struct{}

// Action is part of the Fixer interface.
func (kf *UITestFixer) Action(ctx context.Context, name string) error {
	if name == "Success" {
		return nil
	}
	if name == "Error" {
		return fmt.Errorf("oh no, you pressed the Error button")
	}
	return fmt.Errorf("unknown UITestFixer action: %v", name)
}
