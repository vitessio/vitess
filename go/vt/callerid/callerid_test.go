package callerid

import (
	"testing"
)

func TestFakeCallerID(t *testing.T) {
	im := ImmediateCallerID{
		Username: FakeUsername,
	}
	ef := EffectiveCallerID{
		Principal:    FakePrincipal,
		Component:    FakeComponent,
		Subcomponent: FakeSubcomponent,
	}
	Tests(t, &im, &ef)
}
