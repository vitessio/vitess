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

// Tests performs the necessary testsuite for a CallerID implementation
func Tests(t *testing.T, im *ImmediateCallerID, ef *EffectiveCallerID) {
	ctx := context.TODO()
	ctxim, ok := ImmediateCallerIDFromContext(ctx)
	// For Contexts without ImmediateCallerID, ImmediateCallerIDFromContext should fail
	if ctxim != nil || ok {
		t.Errorf("Expect <nil, false> from ImmediateCallerIDFromContext, but got <%v, %v>", ctxim, ok)
	}
	// For Contexts without EffectiveCallerID, EffectiveCallerIDFromContext should fail
	ctxef, ok := EffectiveCallerIDFromContext(ctx)
	if ctxef != nil || ok {
		t.Errorf("Expect <nil, false> from EffectiveCallerIDFromContext, but got <%v, %v>", ctxef, ok)
	}
	ctx = NewContext(ctx, ef, im)
	ctxim, ok = ImmediateCallerIDFromContext(ctx)
	// If im == nil, ImmediateCallerIDFromContext should fail
	if im == nil && (ctxim != nil || ok) {
		t.Errorf("Expect <nil, false> from ImmediateCallerIDFromContext, but got <%v, %v>", ctxim, ok)
	}
	// retrieved ImmediateCallerID should be equal to the one we put into Context
	if !reflect.DeepEqual(ctxim, im) {
		t.Errorf("Expect <%v, ok> from ImmediateCallerIDFromContext, but got <%v, %v>", im, ctxim, ok)
	}
	if im.GetUsername() != FakeUsername {
		t.Errorf("Expect %v from im.Username(), but got %v", FakeUsername, im.GetUsername())
	}

	ctxef, ok = EffectiveCallerIDFromContext(ctx)
	// If ef == nil, EffectiveCallerIDFromContext should fail
	if ef == nil && (ctxef != nil || ok) {
		t.Errorf("Expect <nil, false> from EffectiveCallerIDFromContext, but got <%v, %v>", ctxef, ok)
	}
	// retrieved EffectiveCallerID should be equal to the one we put into Context
	if !reflect.DeepEqual(ctxef, ef) {
		t.Errorf("Expect <%v, ok> from EffectiveCallerIDFromContext, but got <%v, %v>", ef, ctxef, ok)
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
