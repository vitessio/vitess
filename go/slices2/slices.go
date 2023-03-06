package slices2

// All returns true if all elements return true for given predicate
func All[T any](s []T, fn func(T) bool) bool {
	for _, e := range s {
		if !fn(e) {
			return false
		}
	}
	return true
}

// Any returns true if at least one element returns true for given predicate
func Any[T any](s []T, fn func(T) bool) bool {
	for _, e := range s {
		if fn(e) {
			return true
		}
	}
	return false
}

// AppendToGroup adds the key, value to the given map where each key
// points to a slice of values
func AppendToGroup[M ~map[K][]V, K comparable, V any](m M, k K, v V) {
	lst, ok := m[k]
	if !ok {
		lst = make([]V, 0)
	}
	lst = append(lst, v)
	m[k] = lst
}

// Associate returns a map containing key-value pairs returned by the given
// function applied to the elements of the given slice
func Associate[T, V any, K comparable](s []T, fn func(T) (K, V)) map[K]V {
	ret := make(map[K]V)
	for _, e := range s {
		k, v := fn(e)
		ret[k] = v
	}
	return ret
}

// Chunked splits the slice into a slice of slices, each not exceeding given size
// The last slice might have fewer elements than the given size
func Chunked[T any](s []T, chunkSize int) [][]T {
	sz := len(s)
	ret := make([][]T, 0, sz/chunkSize+2)
	var sub []T
	for i := 0; i < sz; i++ {
		if i%chunkSize == 0 {
			if len(sub) > 0 {
				ret = append(ret, sub)
			}
			sub = make([]T, 0, chunkSize)
		}
		sub = append(sub, s[i])
	}
	if len(sub) > 0 {
		ret = append(ret, sub)
	}
	return ret
}

func ChunkedBy[T any](s []T, fn func(T, T) bool) [][]T {
	ret := make([][]T, 0)
	switch len(s) {
	case 0:
		return ret
	case 1:
		ret = append(ret, []T{s[0]})
		return ret
	}
	var currentSubList = []T{s[0]}
	for _, e := range s[1:] {
		if fn(currentSubList[len(currentSubList)-1], e) {
			currentSubList = append(currentSubList, e)
		} else {
			// save current sub list and start a new one
			ret = append(ret, currentSubList)
			currentSubList = []T{e}
		}
	}
	if len(currentSubList) > 0 {
		ret = append(ret, currentSubList)
	}
	return ret
}

// Distinct returns a slice containing only distinct elements from the given slice
// Elements will retain their original order.
func Distinct[T comparable](s []T) []T {
	m := make(map[T]bool)
	ret := make([]T, 0)
	for _, e := range s {
		_, ok := m[e]
		if ok {
			continue
		}
		m[e] = true
		ret = append(ret, e)
	}
	return ret
}

// DistinctBy returns a slice containing only distinct elements from the
// given slice as distinguished by the given selector function
// Elements will retain their original order.
func DistinctBy[T any, K comparable](s []T, fn func(T) K) []T {
	m := make(map[K]bool)
	ret := make([]T, 0)
	for _, e := range s {
		k := fn(e)
		_, ok := m[k]
		if ok {
			continue
		}
		m[k] = true
		ret = append(ret, e)
	}
	return ret
}

// Drop returns a slice containing all elements except the first n
func Drop[T any](s []T, n int) []T {
	if n >= len(s) {
		return make([]T, 0)
	}
	return s[n:]
}

// DropLast returns a slice containing all elements except the last n
func DropLast[T any](s []T, n int) []T {
	if n >= len(s) {
		return make([]T, 0)
	}
	return s[:len(s)-n]
}

// DropLastWhile returns a slice containing all elements except the last elements
// that satisfy the given predicate
func DropLastWhile[T any](s []T, fn func(T) bool) []T {
	if len(s) == 0 {
		return s
	}
	i := len(s) - 1
	for ; i >= 0; i-- {
		if !fn(s[i]) {
			break
		}
	}
	return s[:i+1]
}

// DropWhile returns a slice containing all elements except the first elements
// that satisfy the given predicate
func DropWhile[T any](s []T, fn func(T) bool) []T {
	if len(s) == 0 {
		return s
	}
	i := 0
	for ; i < len(s); i++ {
		if !fn(s[i]) {
			break
		}
	}
	return s[i:]
}

// Filter returns the slice obtained after retaining only those elements
// in the given slice for which the given function returns true
func Filter[T any](s []T, fn func(T) bool) []T {
	ret := make([]T, 0)
	for _, e := range s {
		if fn(e) {
			ret = append(ret, e)
		}
	}
	return ret
}

func FilterInPlace[T any](s []T, fn func(T) bool) []T {
	filtered := s[:0]
	for _, e := range s {
		if fn(e) {
			filtered = append(filtered, e)
		}
	}
	return filtered
}

// FilterIndexed returns the slice obtained after retaining only those elements
// in the given slice for which the given function returns true. Predicate
// receives the value as well as its index in the slice.
func FilterIndexed[T any](s []T, fn func(int, T) bool) []T {
	ret := make([]T, 0)
	for i, e := range s {
		if fn(i, e) {
			ret = append(ret, e)
		}
	}
	return ret
}

// FilterMap returns the slice obtained after both filtering and mapping using
// the given function. The function should return two values -
// first, the result of the mapping operation and
// second, whether the element should be included or not.
// This is faster than doing a separate filter and map operations,
// since it avoids extra allocations and slice traversals.
func FilterMap[T1, T2 any](
	s []T1,
	fn func(T1) (T2, bool),
) []T2 {

	ret := make([]T2, 0)
	for _, e := range s {
		m, ok := fn(e)
		if ok {
			ret = append(ret, m)
		}
	}
	return ret
}

func Find[T any](s []T, fn func(T) bool) (T, bool) {
	for _, e := range s {
		if fn(e) {
			return e, true
		}
	}
	return *new(T), false
}

// Fold accumulates values starting with given initial value and applying
// given function to current accumulator and each element.
func Fold[T, R any](s []T, initial R, fn func(R, T) R) R {
	acc := initial
	for _, e := range s {
		acc = fn(acc, e)
	}
	return acc
}

// FoldIndexed accumulates values starting with given initial value and applying
// given function to current accumulator and each element. Function also
// receives index of current element.
func FoldIndexed[T, R any](s []T, initial R, fn func(int, R, T) R) R {
	acc := initial
	for i, e := range s {
		acc = fn(i, acc, e)
	}
	return acc
}

// FoldItems accumulates values starting with given intial value and applying
// given function to current accumulator and each key, value.
func FoldItems[M ~map[K]V, K comparable, V, R any](
	m M,
	initial R,
	fn func(R, K, V) R,
) R {
	acc := initial
	for k, v := range m {
		acc = fn(acc, k, v)
	}
	return acc
}

// GetOrInsert checks if a value corresponding to the given key is present
// in the map. If present it returns the existing value. If not, it invokes the
// given callback function to get a new value for the given key, inserts it in
// the map and returns the new value
func GetOrInsert[M ~map[K]V, K comparable, V any](m M, k K, fn func(K) V) V {
	v, ok := m[k]
	if ok {
		// present, return existing value
		return v
	}
	// not present; get value, insert in map and return the new value
	v = fn(k)
	m[k] = v
	return v
}

// GroupBy returns a map containing key to list of values
// returned by the given function applied to the elements of the given slice
func GroupBy[T, V any, K comparable](
	s []T,
	fn func(T) (K, V),
) map[K][]V {
	ret := make(map[K][]V)
	for _, e := range s {
		k, v := fn(e)
		lst, ok := ret[k]
		if !ok {
			lst = make([]V, 0)
		}
		lst = append(lst, v)
		ret[k] = lst
	}
	return ret
}

// Map returns the slice obtained after applying the given function over every
// element in the given slice
func Map[T1, T2 any](s []T1, fn func(T1) T2) []T2 {
	ret := make([]T2, 0, len(s))
	for _, e := range s {
		ret = append(ret, fn(e))
	}
	return ret
}

// MapIndexed returns the slice obtained after applying the given function over every
// element in the given slice. The function also receives the index of each
// element in the slice.
func MapIndexed[T1, T2 any](s []T1, fn func(int, T1) T2) []T2 {
	ret := make([]T2, 0, len(s))
	for i, e := range s {
		ret = append(ret, fn(i, e))
	}
	return ret
}

// Partition returns two slices where the first slice contains elements for
// which the predicate returned true and the second slice contains elements for
// which it returned false.
func Partition[T any](s []T, fn func(T) bool) ([]T, []T) {
	trueList := make([]T, 0)
	falseList := make([]T, 0)
	for _, e := range s {
		if fn(e) {
			trueList = append(trueList, e)
		} else {
			falseList = append(falseList, e)
		}
	}
	return trueList, falseList
}

// Reduce accumulates the values starting with the first element and applying the
// operation from left to right to the current accumulator value and each element
// The input slice must have at least one element.
func Reduce[T any](s []T, fn func(T, T) T) T {
	if len(s) == 1 {
		return s[0]
	}
	return Fold(s[1:], s[0], fn)
}

// ReduceIndexed accumulates the values starting with the first element and applying the
// operation from left to right to the current accumulator value and each element
// The input slice must have at least one element. The function also receives
// the index of the element.
func ReduceIndexed[T any](s []T, fn func(int, T, T) T) T {
	if len(s) == 1 {
		return s[0]
	}
	acc := s[0]
	for i, e := range s[1:] {
		acc = fn(i+1, acc, e)
	}
	return acc
}

// Reverse reverses the elements of the list in place
func Reverse[T any](s []T) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

// Reversed returns a new list with the elements in reverse order
func Reversed[T any](s []T) []T {
	ret := make([]T, 0, len(s))
	for i := len(s) - 1; i >= 0; i-- {
		ret = append(ret, s[i])
	}
	return ret
}

// Take returns the slice obtained after taking the first n elements from the
// given slice.
// If n is greater than the length of the slice, returns the entire slice
func Take[T any](s []T, n int) []T {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// TakeLast returns the slice obtained after taking the last n elements from the
// given slice.
func TakeLast[T any](s []T, n int) []T {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// TakeLastWhile returns a slice containing the last elements satisfying the given
// predicate
func TakeLastWhile[T any](s []T, fn func(T) bool) []T {
	if len(s) == 0 {
		return s
	}
	i := len(s) - 1
	for ; i >= 0; i-- {
		if !fn(s[i]) {
			break
		}
	}
	return s[i+1:]
}

// TakeWhile returns a list containing the first elements satisfying the
// given predicate
func TakeWhile[T any](s []T, fn func(T) bool) []T {
	if len(s) == 0 {
		return s
	}
	i := 0
	for ; i < len(s); i++ {
		if !fn(s[i]) {
			break
		}
	}
	return s[:i]
}

// TransformMap applies the given function to each key, value in the map,
// and returns a new map of the same type after transforming the keys
// and values depending on the callback functions return values. If the last
// bool return value from the callback function is false, the entry is dropped
func TransformMap[M ~map[K]V, K comparable, V any](
	m M,
	fn func(k K, v V) (K, V, bool),
) M {
	ret := make(map[K]V)
	for k, v := range m {
		newK, newV, include := fn(k, v)
		if include {
			ret[newK] = newV
		}
	}
	return ret
}

// Unzip returns two slices, where the first slice is built from the first
// values of each pair from the input slice, and the second slice is built
// from the second values of each pair
func Unzip[T1 any, T2 any](ps []*Pair[T1, T2]) ([]T1, []T2) {
	l := len(ps)
	s1 := make([]T1, 0, l)
	s2 := make([]T2, 0, l)
	for _, p := range ps {
		s1 = append(s1, p.Fst)
		s2 = append(s2, p.Snd)
	}
	return s1, s2
}

// Windowed returns a slice of sliding windows into the given slice of the
// given size, and with the given step
func Windowed[T any](s []T, size, step int) [][]T {
	ret := make([][]T, 0)
	sz := len(s)
	if sz == 0 {
		return ret
	}
	start := 0
	end := 0
	updateEnd := func() {
		e := start + size
		if e >= sz {
			e = sz
		}
		end = e
	}
	updateStart := func() {
		s := start + step
		if s >= sz {
			s = sz
		}
		start = s
	}
	updateEnd()

	for {
		sub := make([]T, 0, end)
		for i := start; i < end; i++ {
			sub = append(sub, s[i])
		}
		ret = append(ret, sub)
		updateStart()
		updateEnd()
		if start == end {
			break
		}
	}
	return ret
}

// Zip returns a slice of pairs from the elements of both slices with the same
// index. The returned slice has the length of the shortest input slice
func Zip[T1 any, T2 any](s1 []T1, s2 []T2) []*Pair[T1, T2] {
	minLen := len(s1)
	if minLen > len(s2) {
		minLen = len(s2)
	}

	// Allocate enough space to avoid copies and extra allocations
	ret := make([]*Pair[T1, T2], 0, minLen)

	for i := 0; i < minLen; i++ {
		ret = append(ret, &Pair[T1, T2]{
			Fst: s1[i],
			Snd: s2[i],
		})
	}
	return ret
}

// Pair represents a generic pair of two values
type Pair[T1, T2 any] struct {
	Fst T1
	Snd T2
}
