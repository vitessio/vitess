package raft

import (
	"testing"
)

func TestInmemTransportImpl(t *testing.T) {
	var inm interface{} = &InmemTransport{}
	if _, ok := inm.(Transport); !ok {
		t.Fatalf("InmemTransport is not a Transport")
	}
	if _, ok := inm.(LoopbackTransport); !ok {
		t.Fatalf("InmemTransport is not a Loopback Transport")
	}
	if _, ok := inm.(WithPeers); !ok {
		t.Fatalf("InmemTransport is not a WithPeers Transport")
	}
}
