package cache

// nullCache is a no-op cache that does not store items
type nullCache struct{}

// Get never returns anything on the nullCache
func (n *nullCache) Get(_ string) (interface{}, bool) {
	return nil, false
}

// Set is a no-op in the nullCache
func (n *nullCache) Set(_ string, _ interface{}) bool {
	return false
}

// ForEach iterates the nullCache, which is always empty
func (n *nullCache) ForEach(_ func(interface{}) bool) {}

// Delete is a no-op in the nullCache
func (n *nullCache) Delete(_ string) {}

// Clear is a no-op in the nullCache
func (n *nullCache) Clear() {}

// Wait is a no-op in the nullcache
func (n *nullCache) Wait() {}

func (n *nullCache) Len() int {
	return 0
}

// Hits returns number of cache hits since creation
func (n *nullCache) Hits() int64 {
	return 0
}

// Hits returns number of cache misses since creation
func (n *nullCache) Misses() int64 {
	return 0
}

// Capacity returns the capacity of the nullCache, which is always 0
func (n *nullCache) UsedCapacity() int64 {
	return 0
}

// Capacity returns the capacity of the nullCache, which is always 0
func (n *nullCache) MaxCapacity() int64 {
	return 0
}

// SetCapacity sets the capacity of the null cache, which is a no-op
func (n *nullCache) SetCapacity(_ int64) {}

func (n *nullCache) Evictions() int64 {
	return 0
}
