package discovery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			disco, err := New(tt.impl, "testcluster", []string{})
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
	Register("testfactory", nil)

	defer func() {
		err := recover()
		assert.NotNil(t, err)
		assert.IsType(t, "", err)
		assert.Contains(t, err.(string), "factory already registered")
	}()

	// this one panics
	Register("testfactory", nil)
	assert.Equal(t, 1, 2, "double register should have panicked")
}
