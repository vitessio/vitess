package atomic2

import (
	"unsafe"
)

//go:linkname writeBarrier runtime.writeBarrier
var writeBarrier struct {
	enabled bool    // compiler emits a check of this before calling write barrier
	pad     [3]byte // compiler uses 32-bit load for "enabled" field
	needed  bool    // identical to enabled, for now (TODO: dedup)
	alignme uint64  // guarantee alignment so that compiler can use a 32 or 64-bit load
}

//go:linkname atomicwb runtime.atomicwb
//go:nosplit
func atomicwb(ptr *unsafe.Pointer, new unsafe.Pointer)

type PointerAndUint64[T any] struct {
	p unsafe.Pointer
	u uint64
}

//go:nosplit
func loadUint128_(addr *unsafe.Pointer) (pp unsafe.Pointer, uu uint64)

func (x *PointerAndUint64[T]) Load() (*T, uint64) {
	p, u := loadUint128_(&x.p)
	return (*T)(p), u
}

//go:nosplit
func compareAndSwapUint128_(addr *unsafe.Pointer, oldp unsafe.Pointer, oldu uint64, newp unsafe.Pointer, newu uint64) (swapped bool)

//go:nosplit
func compareAndSwapUint128(addr *unsafe.Pointer, oldp unsafe.Pointer, oldu uint64, newp unsafe.Pointer, newu uint64) bool {
	if writeBarrier.enabled {
		atomicwb(addr, newp)
	}
	return compareAndSwapUint128_(addr, oldp, oldu, newp, newu)
}

func (x *PointerAndUint64[T]) CompareAndSwap(oldp *T, oldu uint64, newp *T, newu uint64) bool {
	return compareAndSwapUint128(&x.p, unsafe.Pointer(oldp), oldu, unsafe.Pointer(newp), newu)
}
