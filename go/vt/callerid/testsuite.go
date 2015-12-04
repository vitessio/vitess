package callerid

import (
	"reflect"
	"testing"

	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

const (
	// FakePrincipal is the principal of testing effective CallerID
	FakePrincipal = "TestPrincipal"
	// FakeComponent is the component of testing effective CallerID
	FakeComponent = "TestComponent"
	// FakeSubcomponent is the subcomponent of testing effective CallerID
	FakeSubcomponent = "TestSubcomponent"
	// FakeUsername is the username of testing immediate CallerID
	FakeUsername = "TestUsername"
)

// Tests performs the necessary testsuite for CallerID operations
func Tests(t *testing.T, im *querypb.VTGateCallerID, ef *vtrpcpb.CallerID) {
	ctx := context.TODO()
	ctxim := ImmediateCallerIDFromContext(ctx)
	// For Contexts without immediate CallerID, ImmediateCallerIDFromContext should fail
	if ctxim != nil {
		t.Errorf("Expect nil from ImmediateCallerIDFromContext, but got %v", ctxim)
	}
	// For Contexts without effective CallerID, EffectiveCallerIDFromContext should fail
	ctxef := EffectiveCallerIDFromContext(ctx)
	if ctxef != nil {
		t.Errorf("Expect nil from EffectiveCallerIDFromContext, but got %v", ctxef)
	}

	ctx = NewContext(ctx, nil, nil)
	ctxim = ImmediateCallerIDFromContext(ctx)
	// For Contexts with nil immediate CallerID, ImmediateCallerIDFromContext should fail
	if ctxim != nil {
		t.Errorf("Expect nil from ImmediateCallerIDFromContext, but got %v", ctxim)
	}
	// For Contexts with nil effective CallerID, EffectiveCallerIDFromContext should fail
	ctxef = EffectiveCallerIDFromContext(ctx)
	if ctxef != nil {
		t.Errorf("Expect nil from EffectiveCallerIDFromContext, but got %v", ctxef)
	}

	// Test GetXxx on nil receivers, should get all empty strings
	if u := GetUsername(ctxim); u != "" {
		t.Errorf("Expect empty string from GetUsername(nil), but got %v", u)
	}
	if p := GetPrincipal(ctxef); p != "" {
		t.Errorf("Expect empty string from GetPrincipal(nil), but got %v", p)
	}
	if c := GetComponent(ctxef); c != "" {
		t.Errorf("Expect empty string from GetComponent(nil), but got %v", c)
	}
	if s := GetSubcomponent(ctxef); s != "" {
		t.Errorf("Expect empty string from GetSubcomponent(nil), but got %v", s)
	}

	ctx = NewContext(ctx, ef, im)
	ctxim = ImmediateCallerIDFromContext(ctx)
	// retrieved immediate CallerID should be equal to the one we put into Context
	if !reflect.DeepEqual(ctxim, im) {
		t.Errorf("Expect %v from ImmediateCallerIDFromContext, but got %v", im, ctxim)
	}
	if u := GetUsername(im); u != FakeUsername {
		t.Errorf("Expect %v from GetUsername(im), but got %v", FakeUsername, u)
	}

	ctxef = EffectiveCallerIDFromContext(ctx)
	// retrieved effective CallerID should be equal to the one we put into Context
	if !reflect.DeepEqual(ctxef, ef) {
		t.Errorf("Expect %v from EffectiveCallerIDFromContext, but got %v", ef, ctxef)
	}
	if p := GetPrincipal(ef); p != FakePrincipal {
		t.Errorf("Expect %v from GetPrincipal(ef), but got %v", FakePrincipal, p)
	}
	if c := GetComponent(ef); c != FakeComponent {
		t.Errorf("Expect %v from GetComponent(ef), but got %v", FakeComponent, c)
	}
	if s := GetSubcomponent(ef); s != FakeSubcomponent {
		t.Errorf("Expect %v from GetSubcomponent(ef), but got %v", FakeSubcomponent, s)
	}
}
