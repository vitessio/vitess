package topo

import (
	"context"
	"path"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/vt/topo/events"
)

// UpsertMetadata sets the key/value in the metadata if it doesn't exist, otherwise it updates the content
func (ts *Server) UpsertMetadata(ctx context.Context, key string, val string) error {
	keyPath := path.Join(MetadataPath, key)

	_, _, err := ts.globalCell.Get(ctx, keyPath)
	status := "updated"

	if err != nil {
		if !IsErrType(err, NoNode) {
			return err
		}

		status = "created"
	}

	// nil version means that it will insert if keyPath does not exist
	if _, err := ts.globalCell.Update(ctx, keyPath, []byte(val), nil); err != nil {
		return err
	}

	dispatchEvent(keyPath, status)
	return nil
}

// GetMetadata retrieves all metadata value that matches the given key regular expression. If empty all values are returned.
func (ts *Server) GetMetadata(ctx context.Context, keyFilter string) (map[string]string, error) {
	keys, err := ts.globalCell.ListDir(ctx, MetadataPath, false)
	if err != nil {
		return nil, err
	}

	re := sqlparser.LikeToRegexp(keyFilter)

	result := make(map[string]string)
	for _, k := range keys {
		if !re.MatchString(k.Name) {
			continue
		}

		val, err := ts.getMetadata(ctx, k.Name)
		if err != nil {
			return nil, err
		}
		result[k.Name] = val
	}

	return result, nil
}

// DeleteMetadata deletes the key in the metadata
func (ts *Server) DeleteMetadata(ctx context.Context, key string) error {
	keyPath := path.Join(MetadataPath, key)

	// nil version means that it will insert if keyPath does not exist
	if err := ts.globalCell.Delete(ctx, keyPath, nil); err != nil {
		return err
	}

	dispatchEvent(keyPath, "deleted")
	return nil
}

func (ts *Server) getMetadata(ctx context.Context, key string) (string, error) {
	keyPath := path.Join(MetadataPath, key)
	contents, _, err := ts.globalCell.Get(ctx, keyPath)
	if err != nil {
		return "", err
	}

	return string(contents), nil
}

func dispatchEvent(key string, status string) {
	event.Dispatch(&events.MetadataChange{
		Key:    key,
		Status: status,
	})
}
