package callerid

import (
	"reflect"
	"testing"

	"golang.org/x/net/context"
)

const (
	// FakePrincipal is the principal of testing EffectiveCallerID
	FakePrincipal = "TestPrincipal"
	// FakeComponent is the component of testing EffectiveCallerID
	FakeComponent = "TestComponent"
	// FakeSubcomponent is the subcomponent of testing EffectiveCallerID
	FakeSubcomponent = "TestSubcomponent"
	// FakeUsername is the username of testing ImmediateCallerID
	FakeUsername = "TestUsername"
)

// Tests performs the necessary testsuite for CallerID operations
func Tests(t *testing.T, im *ImmediateCallerID, ef *EffectiveCallerID) {
	ctx := context.TODO()
	ctxim := ImmediateCallerIDFromContext(ctx)
	// For Contexts without ImmediateCallerID, ImmediateCallerIDFromContext should fail
	if ctxim != nil {
		t.Errorf("Expect nil from ImmediateCallerIDFromContext, but got %v", ctxim)
	}
	// For Contexts without EffectiveCallerID, EffectiveCallerIDFromContext should fail
	ctxef := EffectiveCallerIDFromContext(ctx)
	if ctxef != nil {
		t.Errorf("Expect nil from EffectiveCallerIDFromContext, but got %v", ctxef)
	}

	ctx = NewContext(ctx, nil, nil)
	ctxim = ImmediateCallerIDFromContext(ctx)
	// For Contexts with nil ImmediateCallerID, ImmediateCallerIDFromContext should fail
	if ctxim != nil {
		t.Errorf("Expect nil from ImmediateCallerIDFromContext, but got %v", ctxim)
	}
	// For Contexts with nil EffectiveCallerID, EffectiveCallerIDFromContext should fail
	ctxef = EffectiveCallerIDFromContext(ctx)
	if ctxef != nil {
		t.Errorf("Expect nil from EffectiveCallerIDFromContext, but got %v", ctxef)
	}

	// Test GetXxx on nil receivers, should get all empty strings
	if u := ctxim.GetUsername(); u != "" {
		t.Errorf("Expect empty string from (nil).GetUsername(), but got %v", u)
	}
	if p := ctxef.GetPrincipal(); p != "" {
		t.Errorf("Expect empty string from (nil).GetPrincipal(), but got %v", p)
	}
	if c := ctxef.GetComponent(); c != "" {
		t.Errorf("Expect empty string from (nil).GetComponent(), but got %v", c)
	}
	if s := ctxef.GetSubcomponent(); s != "" {
		t.Errorf("Expect empty string from (nil).GetSubcomponent(), but got %v", s)
	}

	ctx = NewContext(ctx, ef, im)
	ctxim = ImmediateCallerIDFromContext(ctx)
	// retrieved ImmediateCallerID should be equal to the one we put into Context
	if !reflect.DeepEqual(ctxim, im) {
		t.Errorf("Expect %v from ImmediateCallerIDFromContext, but got %v", im, ctxim)
	}
	if im.Username != FakeUsername {
		t.Errorf("Expect %v from im.Username(), but got %v", FakeUsername, im.Username)
	}

	ctxef = EffectiveCallerIDFromContext(ctx)
	// retrieved EffectiveCallerID should be equal to the one we put into Context
	if !reflect.DeepEqual(ctxef, ef) {
		t.Errorf("Expect %v from EffectiveCallerIDFromContext, but got %v", ef, ctxef)
	}
	if ef.GetPrincipal() != FakePrincipal {
		t.Errorf("Expect %v from ef.Principal(), but got %v", FakePrincipal, ef.GetPrincipal())
	}
	if ef.GetComponent() != FakeComponent {
		t.Errorf("Expect %v from ef.Component(), but got %v", FakeComponent, ef.GetComponent())
	}
	if ef.GetSubcomponent() != FakeSubcomponent {
		t.Errorf("Expect %v from ef.Subcomponent(), but got %v", FakeSubcomponent, ef.GetSubcomponent())
	}
}
