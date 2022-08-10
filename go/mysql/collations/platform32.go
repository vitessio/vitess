//go:build 386 || arm || mips || mipsle || wasm

/*
Copyright 2021 The Vitess Authors.

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

package collations

// 4-byte constants for hashing in 32-bit systems
const (
	m1 = 0x78bd642f
	m2 = 0xa0b428db
	m3 = 0x9c88c6e3
	m4 = 0x75374cc3
	m5 = 0xc47d124f
)

// Generate all the metadata used for collations from the JSON data dumped from MySQL
// In 32 bit systems, do not use memory embedding, it's not worth it
//go:generate go run ./tools/makecolldata/ --embed=false
