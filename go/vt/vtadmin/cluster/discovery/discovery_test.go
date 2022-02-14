package discovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

func TestNew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		impl string
		err  error
		typ  Discovery
	}{
		{
			name: "success",
			impl: "consul",
			err:  nil,
			typ:  &ConsulDiscovery{},
		},
		{
			name: "unregistered",
			impl: "unregistered",
			err:  ErrImplementationNotRegistered,
			typ:  nil,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			disco, err := New(tt.impl, &vtadminpb.Cluster{Id: "testid", Name: "testcluster"}, []string{})
			if tt.err != nil {
				assert.Error(t, err, tt.err.Error())
				return
			}

			assert.NoError(t, err)
			assert.IsType(t, tt.typ, disco)
		})
	}
}

func TestRegister(t *testing.T) {
	t.Parallel()

	// Use a timestamp to allow running tests with `-count=N`.
	ts := time.Now().UnixNano()
	factoryName := fmt.Sprintf("testfactory-%d", ts)

	Register(factoryName, nil)

	defer func() {
		err := recover()
		assert.NotNil(t, err)
		assert.IsType(t, "", err)
		assert.Contains(t, err.(string), "factory already registered")
	}()

	// this one panics
	Register(factoryName, nil)
	assert.Equal(t, 1, 2, "double register should have panicked")
}
