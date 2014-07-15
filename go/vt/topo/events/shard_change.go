package events

// ShardChange is an event that describes changes to a shard.
type ShardChange struct {
	Keyspace string
	Shard    string
	Status   string
	// Data optionally contains the updated lockserver file data for the shard.
	Data string
}
