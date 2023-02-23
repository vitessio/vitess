/*
Copyright 2023 The Vitess Authors.

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

// Based on https://github.com/jandrewrogers/MetroHash

package metro

import (
	"encoding/binary"
	"math/bits"
)

const k0 = 0xC83A91E1
const k1 = 0x8648DBDB
const k2 = 0x7BDEC03B
const k3 = 0x2F5870A5

type Metro128 struct {
	state [4]uint64
	input [32]byte
	count int
}

func (m *Metro128) Init(seed uint64) {
	m.state[0] = (seed - k0) * k3
	m.state[1] = (seed + k1) * k2
	m.state[2] = (seed + k0) * k2
	m.state[3] = (seed - k1) * k3
	m.count = 0
}

func (m *Metro128) Reset() {
	m.Init(0)
}

func (m *Metro128) Write8(u uint8) {
	var scratch = [1]byte{u}
	_, _ = m.Write(scratch[:1])
}

func (m *Metro128) Write16(u uint16) {
	var scratch [2]byte
	binary.LittleEndian.PutUint16(scratch[:2], u)
	_, _ = m.Write(scratch[:2])
}

func (m *Metro128) Write32(u uint32) {
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[:4], u)
	_, _ = m.Write(scratch[:4])
}

func (m *Metro128) Write64(u uint64) {
	var scratch [8]byte
	binary.LittleEndian.PutUint64(scratch[:8], u)
	_, _ = m.Write(scratch[:8])
}

func (m *Metro128) Write(buffer []byte) (int, error) {
	ptr := buffer

	// input buffer may be partially filled
	if fill := m.count % 32; fill != 0 {
		fill = 32 - fill
		if fill > len(buffer) {
			fill = len(buffer)
		}

		copy(m.input[m.count%32:], ptr[:fill])
		ptr = ptr[fill:]
		m.count += fill

		if m.count%32 != 0 {
			return len(buffer), nil
		}

		// process full input buffer
		m.state[0] += binary.LittleEndian.Uint64(m.input[0:8]) * k0
		m.state[0] = bits.RotateLeft64(m.state[0], -29) + m.state[2]
		m.state[1] += binary.LittleEndian.Uint64(m.input[8:16]) * k1
		m.state[1] = bits.RotateLeft64(m.state[1], -29) + m.state[3]
		m.state[2] += binary.LittleEndian.Uint64(m.input[16:24]) * k2
		m.state[2] = bits.RotateLeft64(m.state[2], -29) + m.state[0]
		m.state[3] += binary.LittleEndian.Uint64(m.input[24:32]) * k3
		m.state[3] = bits.RotateLeft64(m.state[3], -29) + m.state[1]
	}

	m.count += len(ptr)
	for len(ptr) >= 32 {
		m.state[0] += binary.LittleEndian.Uint64(ptr[0:8]) * k0
		m.state[0] = bits.RotateLeft64(m.state[0], -29) + m.state[2]
		m.state[1] += binary.LittleEndian.Uint64(ptr[8:16]) * k1
		m.state[1] = bits.RotateLeft64(m.state[1], -29) + m.state[3]
		m.state[2] += binary.LittleEndian.Uint64(ptr[16:24]) * k2
		m.state[2] = bits.RotateLeft64(m.state[2], -29) + m.state[0]
		m.state[3] += binary.LittleEndian.Uint64(ptr[24:32]) * k3
		m.state[3] = bits.RotateLeft64(m.state[3], -29) + m.state[1]

		ptr = ptr[32:]
	}
	if len(ptr) > 0 {
		copy(m.input[:], ptr)
	}
	return len(buffer), nil
}

func (m *Metro128) Sum64() uint64 {
	m.finalize()
	return m.state[0]
}

func (m *Metro128) Sum128() (hash [16]byte) {
	m.finalize()
	binary.LittleEndian.PutUint64(hash[0:8], m.state[0])
	binary.LittleEndian.PutUint64(hash[8:16], m.state[1])
	return
}

func (m *Metro128) finalize() {
	if m.count >= 32 {
		m.state[2] ^= bits.RotateLeft64(((m.state[0]+m.state[3])*k0)+m.state[1], -21) * k1
		m.state[3] ^= bits.RotateLeft64(((m.state[1]+m.state[2])*k1)+m.state[0], -21) * k0
		m.state[0] ^= bits.RotateLeft64(((m.state[0]+m.state[2])*k0)+m.state[3], -21) * k1
		m.state[1] ^= bits.RotateLeft64(((m.state[1]+m.state[3])*k1)+m.state[2], -21) * k0
	}

	ptr := m.input[:m.count%32]
	if len(ptr) >= 16 {
		m.state[0] += binary.LittleEndian.Uint64(ptr[0:8]) * k2
		m.state[0] = bits.RotateLeft64(m.state[0], -33) * k3
		m.state[1] += binary.LittleEndian.Uint64(ptr[8:16]) * k2
		m.state[1] = bits.RotateLeft64(m.state[1], -33) * k3
		m.state[0] ^= bits.RotateLeft64((m.state[0]*k2)+m.state[1], -45) * k1
		m.state[1] ^= bits.RotateLeft64((m.state[1]*k3)+m.state[0], -45) * k0

		ptr = ptr[16:]
	}

	if len(ptr) >= 8 {
		m.state[0] += binary.LittleEndian.Uint64(ptr[:8]) * k2
		m.state[0] = bits.RotateLeft64(m.state[0], -33) * k3
		m.state[0] ^= bits.RotateLeft64((m.state[0]*k2)+m.state[1], -27) * k1

		ptr = ptr[8:]
	}

	if len(ptr) >= 4 {
		m.state[1] += uint64(binary.LittleEndian.Uint32(ptr[:4])) * k2
		m.state[1] = bits.RotateLeft64(m.state[1], -33) * k3
		m.state[1] ^= bits.RotateLeft64((m.state[1]*k3)+m.state[0], -46) * k0

		ptr = ptr[4:]
	}

	if len(ptr) >= 2 {
		m.state[0] += uint64(binary.LittleEndian.Uint16(ptr)) * k2
		m.state[0] = bits.RotateLeft64(m.state[0], -33) * k3
		m.state[0] ^= bits.RotateLeft64((m.state[0]*k2)+m.state[1], -22) * k1

		ptr = ptr[2:]
	}

	if len(ptr) >= 1 {
		m.state[1] += uint64(ptr[0]) * k2
		m.state[1] = bits.RotateLeft64(m.state[1], -33) * k3
		m.state[1] ^= bits.RotateLeft64((m.state[1]*k3)+m.state[0], -58) * k0
	}

	m.state[0] += bits.RotateLeft64((m.state[0]*k0)+m.state[1], -13)
	m.state[1] += bits.RotateLeft64((m.state[1]*k1)+m.state[0], -37)
	m.state[0] += bits.RotateLeft64((m.state[0]*k2)+m.state[1], -13)
	m.state[1] += bits.RotateLeft64((m.state[1]*k3)+m.state[0], -37)
}
