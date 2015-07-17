package callerid

import (
	"testing"
)

func TestFakeCallerID(t *testing.T) {
	im := ImmediateCallerID{
		username: FakeUsername,
	}
	ef := EffectiveCallerID{
		principal:    FakePrincipal,
		component:    FakeComponent,
		subComponent: FakeSubcomponent,
	}
	Tests(t, &im, &ef)
}
