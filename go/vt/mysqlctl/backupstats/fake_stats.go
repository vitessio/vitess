package backupstats

import "time"

type FakeStats struct {
	ScopeV              map[ScopeType]ScopeValue
	TimedIncrementCalls []time.Duration
	ScopeCalls          [][]Scope
	ScopeReturns        []Stats
}

func NewFakeStats(scopes ...Scope) *FakeStats {
	scopeV := make(map[ScopeType]ScopeValue)
	for _, s := range scopes {
		scopeV[s.Type] = s.Value
	}
	return &FakeStats{
		ScopeV: scopeV,
	}
}

func (fs *FakeStats) Scope(scopes ...Scope) Stats {
	fs.ScopeCalls = append(fs.ScopeCalls, scopes)
	newScopeV := map[ScopeType]ScopeValue{}
	for t, v := range fs.ScopeV {
		newScopeV[t] = v
	}
	for _, s := range scopes {
		if _, ok := newScopeV[s.Type]; !ok {
			newScopeV[s.Type] = s.Value
		}
	}
	newScopes := []Scope{}
	for t, v := range newScopeV {
		newScopes = append(newScopes, Scope{t, v})
	}
	sfs := NewFakeStats(newScopes...)
	fs.ScopeReturns = append(fs.ScopeReturns, sfs)
	return sfs
}

func (fs *FakeStats) TimedIncrement(d time.Duration) {
	fs.TimedIncrementCalls = append(fs.TimedIncrementCalls, d)
}
