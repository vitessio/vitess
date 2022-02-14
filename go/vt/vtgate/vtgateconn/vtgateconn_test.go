package vtgateconn

import (
	"testing"

	"context"
)

func TestRegisterDialer(t *testing.T) {
	dialerFunc := func(context.Context, string) (Impl, error) {
		return nil, nil
	}
	RegisterDialer("test1", dialerFunc)
	RegisterDialer("test1", dialerFunc)
}

func TestGetDialerWithProtocol(t *testing.T) {
	protocol := "test2"
	_, err := DialProtocol(context.Background(), protocol, "")
	if err == nil || err.Error() != "no dialer registered for VTGate protocol "+protocol {
		t.Fatalf("protocol: %s is not registered, should return error: %v", protocol, err)
	}
	RegisterDialer(protocol, func(context.Context, string) (Impl, error) {
		return nil, nil
	})
	c, err := DialProtocol(context.Background(), protocol, "")
	if err != nil || c == nil {
		t.Fatalf("dialerFunc has been registered, should not get nil: %v %v", err, c)
	}
}
