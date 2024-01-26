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

package sidecar

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetCreateQuery(t *testing.T) {
	originalName := GetName()
	defer func() {
		SetName(originalName)
	}()

	require.Equal(t, "_vt", GetIdentifier())
	require.Equal(t, "create database if not exists _vt", GetCreateQuery())
	SetName("test")
	require.Equal(t, "test", GetIdentifier())
	require.Equal(t, "create database if not exists test", GetCreateQuery())
}
