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

package decimal

// Our weight string format is normalizing the weight string to a fixed length,
// so it becomes byte-ordered. The byte lengths are pre-computed based on
// https://dev.mysql.com/doc/refman/8.0/en/fixed-point-types.html
// and generated empirically with a manual loop:
//
//	for i := 1; i <= 65; i++ {
//		dec, err := NewFromMySQL(bytes.Repeat([]byte("9"), i))
//		if err != nil {
//			t.Fatal(err)
//		}
//
//		byteLengths = append(byteLengths, len(dec.value.Bytes()))
//	}
var weightStringLengths = []int{
	0, 1, 1, 2, 2, 3, 3, 3, 4, 4, 5, 5, 5, 6, 6, 7, 7, 8, 8, 8,
	9, 9, 10, 10, 10, 11, 11, 12, 12, 13, 13, 13, 14, 14, 15, 15, 15,
	16, 16, 17, 17, 18, 18, 18, 19, 19, 20, 20, 20, 21, 21, 22, 22,
	23, 23, 23, 24, 24, 25, 25, 25, 26, 26, 27, 27, 27,
}

func (d Decimal) WeightString(dst []byte, length, precision int32) []byte {
	dec := d.rescale(-precision)
	dec = dec.Clamp(length-precision, precision)

	buf := make([]byte, weightStringLengths[length]+1)
	dec.value.FillBytes(buf[:])

	if dec.value.Sign() < 0 {
		for i := range buf {
			buf[i] ^= 0xff
		}
	}
	// Use the same trick as used for signed numbers on the first byte.
	buf[0] ^= 0x80

	dst = append(dst, buf[:]...)
	return dst
}
