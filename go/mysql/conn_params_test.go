/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

import (
	"testing"

	"vitess.io/vitess/go/vt/vttls"

	"github.com/stretchr/testify/assert"
)

func TestConnParams_EnableSSL(t *testing.T) {
	p := ConnParams{}
	p.EnableSSL()
	assert := assert.New(t)
	assert.EqualValues(vttls.VerifyIdentity, p.SslMode, "should enable strictest mode")
	assert.EqualValues(vttls.VerifyIdentity, p.EffectiveSslMode(), "should enable strictest mode")
}

func TestConnParams_EffectiveSslModeLegacyFlags(t *testing.T) {
	p := ConnParams{
		Flags: CapabilityClientSSL,
	}
	assert := assert.New(t)
	assert.EqualValues("", p.SslMode, "should enable strictest mode")
	assert.EqualValues(vttls.VerifyIdentity, p.EffectiveSslMode(), "should enable strictest mode")
}

func TestConnParams_EffectiveSslModeConfigured(t *testing.T) {
	p := ConnParams{
		SslMode: vttls.VerifyCA,
		Flags:   CapabilityClientSSL,
	}
	assert := assert.New(t)
	assert.EqualValues(vttls.VerifyCA, p.SslMode, "should use verify_ca")
	assert.EqualValues(vttls.VerifyCA, p.EffectiveSslMode(), "should use configured mode")
}

func TestConnParams_SslEnabledNotConfigured(t *testing.T) {
	p := ConnParams{}
	assert := assert.New(t)
	assert.False(p.SslEnabled())
	assert.EqualValues("", p.SslMode, "should be empty")
	assert.EqualValues(vttls.Disabled, p.EffectiveSslMode(), "should have ssl disabled")
}

func TestConnParams_SslEnabledPreferredUnixSocket(t *testing.T) {
	p := ConnParams{
		SslMode:    vttls.Preferred,
		UnixSocket: "/tmp/mysql.sock",
	}
	assert := assert.New(t)
	assert.False(p.SslEnabled())
}

func TestConnParams_SslEnabledPreferredWithHost(t *testing.T) {
	p := ConnParams{
		Host:    "localhost",
		SslMode: vttls.Preferred,
	}
	assert := assert.New(t)
	assert.True(p.SslEnabled())
}

func TestConnParams_SslEnabledDisabled(t *testing.T) {
	p := ConnParams{
		SslMode: vttls.Disabled,
	}
	assert := assert.New(t)
	assert.False(p.SslEnabled())
}

func TestConnParams_SslEnabledRequired(t *testing.T) {
	p := ConnParams{
		SslMode: vttls.Required,
	}
	assert := assert.New(t)
	assert.True(p.SslEnabled())
}

func TestConnParams_SslEnabledVerifyCA(t *testing.T) {
	p := ConnParams{
		SslMode: vttls.VerifyCA,
	}
	assert := assert.New(t)
	assert.True(p.SslEnabled())
}

func TestConnParams_SslEnabledVerifyIdentity(t *testing.T) {
	p := ConnParams{
		SslMode: vttls.VerifyIdentity,
	}
	assert := assert.New(t)
	assert.True(p.SslEnabled())
}

func TestConnParams_SslRequiredDisabled(t *testing.T) {
	p := ConnParams{
		SslMode: vttls.Disabled,
	}
	assert := assert.New(t)
	assert.False(p.SslRequired())
}

func TestConnParams_SslRequiredPreferred(t *testing.T) {
	p := ConnParams{
		SslMode: vttls.Preferred,
	}
	assert := assert.New(t)
	assert.False(p.SslRequired())
}

func TestConnParams_SslRequiredRequired(t *testing.T) {
	p := ConnParams{
		SslMode: vttls.Required,
	}
	assert := assert.New(t)
	assert.True(p.SslRequired())
}

func TestConnParams_SslRequiredVerifyCA(t *testing.T) {
	p := ConnParams{
		SslMode: vttls.VerifyCA,
	}
	assert := assert.New(t)
	assert.True(p.SslRequired())
}

func TestConnParams_SslRequiredVerifyIdentity(t *testing.T) {
	p := ConnParams{
		SslMode: vttls.VerifyIdentity,
	}
	assert := assert.New(t)
	assert.True(p.SslRequired())
}
