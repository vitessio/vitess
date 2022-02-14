package events

// MetadataChange is an event that describes changes to topology metadata
type MetadataChange struct {
	Key    string
	Status string
}
