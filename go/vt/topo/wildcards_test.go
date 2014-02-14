package topo

import (
	"fmt"
	"testing"
)

type fakeWildcardBackend struct {
	keyspaces []string
	shards    map[string][]string
}

func (fwb *fakeWildcardBackend) GetKeyspaces() ([]string, error) {
	if fwb.keyspaces == nil {
		return nil, fmt.Errorf("fake error")
	}
	return fwb.keyspaces, nil
}

func (fwb *fakeWildcardBackend) GetShard(keyspace, shard string) (*ShardInfo, error) {
	shards, ok := fwb.shards[keyspace]
	if !ok {
		return nil, ErrNoNode
	}
	if shards == nil {
		return nil, fmt.Errorf("fake error")
	}
	for _, s := range shards {
		if s == shard {
			return nil, nil
		}
	}
	return nil, ErrNoNode
}

func (fwb *fakeWildcardBackend) GetShardNames(keyspace string) ([]string, error) {
	shards, ok := fwb.shards[keyspace]
	if !ok {
		return nil, ErrNoNode
	}
	if shards == nil {
		return nil, fmt.Errorf("fake error")
	}
	return shards, nil
}

func validateKeyspaceWildcard(t *testing.T, fwb *fakeWildcardBackend, param string, expected []string) {
	r, err := ResolveKeyspaceWildcard(fwb, param)
	if err != nil {
		if expected != nil {
			t.Errorf("was not expecting an error but got: %v", err)
		}
		return
	}

	if len(r) != len(expected) {
		t.Errorf("got wrong result: %v", r)
		return
	}
	for i, e := range expected {
		if r[i] != e {
			t.Errorf("got wrong result[%v]: %v", i, r)
		}
	}
}

func TestKeyspaceWildcards(t *testing.T) {
	fwb := &fakeWildcardBackend{
		keyspaces: []string{"aaaaa", "aabbb", "bbbbb"},
	}
	validateKeyspaceWildcard(t, fwb, "*", []string{"aaaaa", "aabbb", "bbbbb"})
	validateKeyspaceWildcard(t, fwb, "aa*", []string{"aaaaa", "aabbb"})
	validateKeyspaceWildcard(t, fwb, "??b??", []string{"aabbb", "bbbbb"})
	validateKeyspaceWildcard(t, fwb, "ccc", []string{"ccc"})

	validateKeyspaceWildcard(t, fwb, "ccc\\", nil)
}

func validateShardWildcard(t *testing.T, fwb *fakeWildcardBackend, param string, expected []KeyspaceShard) {
	r, err := ResolveShardWildcard(fwb, param)
	if err != nil {
		if expected != nil {
			t.Errorf("was not expecting an error but got: %v", err)
		}
		return
	}

	if len(r) != len(expected) {
		t.Errorf("got wrong result: %v", r)
		return
	}
	for i, e := range expected {
		if r[i] != e {
			t.Errorf("got wrong result[%v]: %v", i, r)
		}
	}
}

func TestShardWildcards(t *testing.T) {
	fwb := &fakeWildcardBackend{
		keyspaces: []string{"aaaaa", "bbbbb"},
		shards: map[string][]string{
			"aaaaa": []string{"s0", "s1"},
			"bbbbb": []string{"-40", "40-80", "80-C0", "C0-"},
		},
	}
	validateShardWildcard(t, fwb, "*/*", []KeyspaceShard{
		KeyspaceShard{"aaaaa", "s0"},
		KeyspaceShard{"aaaaa", "s1"},
		KeyspaceShard{"bbbbb", "-40"},
		KeyspaceShard{"bbbbb", "40-80"},
		KeyspaceShard{"bbbbb", "80-C0"},
		KeyspaceShard{"bbbbb", "C0-"},
	})
	validateShardWildcard(t, fwb, "aaaaa/*", []KeyspaceShard{
		KeyspaceShard{"aaaaa", "s0"},
		KeyspaceShard{"aaaaa", "s1"},
	})
	validateShardWildcard(t, fwb, "*/s1", []KeyspaceShard{
		KeyspaceShard{"aaaaa", "s1"},
	})
	validateShardWildcard(t, fwb, "*/*0*", []KeyspaceShard{
		KeyspaceShard{"aaaaa", "s0"},
		KeyspaceShard{"bbbbb", "-40"},
		KeyspaceShard{"bbbbb", "40-80"},
		KeyspaceShard{"bbbbb", "80-C0"},
		KeyspaceShard{"bbbbb", "C0-"},
	})
	validateShardWildcard(t, fwb, "aaaaa/ccccc", []KeyspaceShard{
		KeyspaceShard{"aaaaa", "ccccc"},
	})
	validateShardWildcard(t, fwb, "ccccc/s0", []KeyspaceShard{
		KeyspaceShard{"ccccc", "s0"},
	})

	// error cases
	fwb = &fakeWildcardBackend{
		keyspaces: []string{"aaaaa", "bbbbb"},
		shards: map[string][]string{
			"aaaaa": nil,
		},
	}

	// these two will return an error as GetShardNames("aaaaa")
	// will return an error.
	validateShardWildcard(t, fwb, "aaaaa/bbbb*", nil)
	validateShardWildcard(t, fwb, "aaaa*/bbbb*", nil)

	// GetShardNames("bbbbb") will return ErrNoNode, so we get empty lists
	// in this case, as the keyspace is a wildcard.
	validateShardWildcard(t, fwb, "bbbb*/cccc*", []KeyspaceShard{})

	// GetShardNames("bbbbb") returns ErrNoNode, so we get an error
	// in this case, as keyspace is not a wildcard.
	validateShardWildcard(t, fwb, "bbbbb/cccc*", nil)

	// GetKeyspaces() will fail hard in this one, so we get an error
	fwb = &fakeWildcardBackend{}
	validateShardWildcard(t, fwb, "*/s1", nil)

	// GetKeyspaces() will return an empty list, so no error, no result
	fwb = &fakeWildcardBackend{
		keyspaces: []string{},
	}
	validateShardWildcard(t, fwb, "*/s1", []KeyspaceShard{})
}
