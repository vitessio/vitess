package callerid

import (
	"golang.org/x/net/context"
	"reflect"
	"testing"
)

type fakeImmediateCallerID struct {
	userName string
}

func (fim *fakeImmediateCallerID) GetUsername() string {
	return fim.userName
}

type fakeEffectiveCallerID struct {
	principal    string
	component    string
	subComponent string
}

func (fef *fakeEffectiveCallerID) GetPrincipal() string {
	return fef.principal
}

func (fef *fakeEffectiveCallerID) GetComponent() string {
	return fef.component
}

func (fef *fakeEffectiveCallerID) GetSubcomponent() string {
	return fef.subComponent
}

func TestFakeCallerID(t *testing.T) {
	im := fakeImmediateCallerID{
		userName: "vitess",
	}
	ef := fakeEffectiveCallerID{
		principal:    "third_party",
		component:    "a big process",
		subComponent: "a huge servlet",
	}
	CallerIDTests(t, &im, &ef)
}

func CallerIDTests(t *testing.T, im ImmediateCallerID, ef EffectiveCallerID) {
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
	ctxef, ok = EffectiveCallerIDFromContext(ctx)
	// If ef == nil, EffectiveCallerIDFromContext should fail
	if ef == nil && (ctxef != nil || ok) {
		t.Errorf("Expect <nil, false> from EffectiveCallerIDFromContext, but got <%v, %v>", ctxef, ok)
	}
	// retrieved EffectiveCallerID should be equal to the one we put into Context
	if !reflect.DeepEqual(ctxef, ef) {
		t.Errorf("Expect <%v, ok> from EffectiveCallerIDFromContext, but got <%v, %v>", ef, ctxef, ok)
	}
}
