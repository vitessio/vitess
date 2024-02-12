/*
Copyright 2024 The Vitess Authors.

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

package grpcclient

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestAppendStaticAuth(t *testing.T) {
	{
		clientCreds = nil
		clientCredsErr = nil
		opts, err := AppendStaticAuth([]grpc.DialOption{})
		assert.Nil(t, err)
		assert.Len(t, opts, 0)
	}
	{
		clientCreds = nil
		clientCredsErr = errors.New("test err")
		opts, err := AppendStaticAuth([]grpc.DialOption{})
		assert.NotNil(t, err)
		assert.Len(t, opts, 0)
	}
	{
		clientCreds = &StaticAuthClientCreds{Username: "test", Password: "123456"}
		clientCredsErr = nil
		opts, err := AppendStaticAuth([]grpc.DialOption{})
		assert.Nil(t, err)
		assert.Len(t, opts, 1)
	}
}

func TestGetStaticAuthCreds(t *testing.T) {
	tmp, err := os.CreateTemp("", t.Name())
	assert.Nil(t, err)
	defer os.Remove(tmp.Name())
	credsFile = tmp.Name()
	clientCredsSigChan = make(chan os.Signal, 1)

	// load old creds
	fmt.Fprint(tmp, `{"Username": "old", "Password": "123456"}`)
	ResetStaticAuth()
	creds, err := getStaticAuthCreds()
	assert.Nil(t, err)
	assert.Equal(t, &StaticAuthClientCreds{Username: "old", Password: "123456"}, creds)

	// write new creds to the same file
	_ = tmp.Truncate(0)
	_, _ = tmp.Seek(0, 0)
	fmt.Fprint(tmp, `{"Username": "new", "Password": "123456789"}`)

	// test the creds did not change yet
	creds, err = getStaticAuthCreds()
	assert.Nil(t, err)
	assert.Equal(t, &StaticAuthClientCreds{Username: "old", Password: "123456"}, creds)

	// test SIGHUP signal triggers reload
	credsOld := creds
	clientCredsSigChan <- syscall.SIGHUP
	timeoutChan := time.After(time.Second * 10)
	for {
		select {
		case <-timeoutChan:
			assert.Fail(t, "timed out waiting for SIGHUP reload of static auth creds")
			return
		default:
			// confirm new creds get loaded
			creds, err = getStaticAuthCreds()
			if reflect.DeepEqual(creds, credsOld) {
				continue // not changed yet
			}
			assert.Nil(t, err)
			assert.Equal(t, &StaticAuthClientCreds{Username: "new", Password: "123456789"}, creds)
			return
		}
	}
}

func TestLoadStaticAuthCredsFromFile(t *testing.T) {
	{
		f, err := os.CreateTemp("", t.Name())
		if !assert.Nil(t, err) {
			assert.FailNowf(t, "cannot create temp file: %s", err.Error())
		}
		defer os.Remove(f.Name())
		fmt.Fprint(f, `{
			"Username": "test",
			"Password": "correct horse battery staple"
		}`)
		if !assert.Nil(t, err) {
			assert.FailNowf(t, "cannot read auth file: %s", err.Error())
		}

		creds, err := loadStaticAuthCredsFromFile(f.Name())
		assert.Nil(t, err)
		assert.Equal(t, "test", creds.Username)
		assert.Equal(t, "correct horse battery staple", creds.Password)
	}
	{
		_, err := loadStaticAuthCredsFromFile(`does-not-exist`)
		assert.NotNil(t, err)
	}
}
