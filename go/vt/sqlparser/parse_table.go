package sqlparser

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// ParseTable parses the input as a qualified table name.
// It handles all valid literal escaping.
func ParseTable(input string) (keyspace, table string, err error) {
	tokenizer := NewStringTokenizer(input)

	// Start, want ID
	token, value := tokenizer.Scan()
	switch token {
	case ID:
		table = string(value)
	default:
		table = KeywordString(token)
		if table == "" {
			return "", "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid table name: %s", input)
		}
	}

	// Seen first ID, want '.' or 0
	token, _ = tokenizer.Scan()
	switch token {
	case '.':
		keyspace = table
	case 0:
		return keyspace, table, nil
	default:
		return "", "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid table name: %s", input)
	}

	// Seen '.', want ID
	token, value = tokenizer.Scan()
	switch token {
	case ID:
		table = string(value)
	default:
		table = KeywordString(token)
		if table == "" {
			return "", "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid table name: %s", input)
		}
	}

	// Seen second ID, want 0
	token, _ = tokenizer.Scan()
	switch token {
	case 0:
		return keyspace, table, nil
	default:
		return "", "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid table name: %s", input)
	}
}
