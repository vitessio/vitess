package events

// KeyspaceChange is an event that describes changes to a keyspace.
type KeyspaceChange struct {
	Keyspace string
	Status   string
	// Data optionally contains the updated lockserver file data for the keyspace.
	Data string
}
