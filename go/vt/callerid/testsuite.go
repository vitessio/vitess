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
	ctx = NewContext(ctx, ef, im)

	ctxim = ImmediateCallerIDFromContext(ctx)
	// retrieved ImmediateCallerID should be equal to the one we put into Context
	if !reflect.DeepEqual(ctxim, im) {
		t.Errorf("Expect %v from ImmediateCallerIDFromContext, but got %v", im, ctxim)
	}
	if im.GetUsername() != FakeUsername {
		t.Errorf("Expect %v from im.Username(), but got %v", FakeUsername, im.GetUsername())
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
