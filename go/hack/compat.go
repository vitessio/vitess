//go:build !gc || wasm

package hack

func String(b []byte) (s string) {
	return string(b)
}

func StringBytes(s string) []byte {
	return []byte(s)
}

func DisableProtoBufRandomness() {}

func RuntimeMemhash(b []byte, hash uint64) uint64 {
	for i := 0; i < len(b); i++ {
		hash *= 1099511628211
		hash ^= uint64(b[i])
	}
	return hash
}

func RuntimeStrhash(str string, hash uint64) uint64 {
	for i := 0; i < len(str); i++ {
		hash *= 1099511628211
		hash ^= uint64(str[i])
	}
	return hash
}

//go:linkname ParseFloatPrefix strconv.parseFloatPrefix
func ParseFloatPrefix(s string, bitSize int) (float64, int, error)

func RuntimeAllocSize(size int64) int64 {
	return size
}
