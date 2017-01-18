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
