package utils

// utils.go contains general utility functions used in the splitquery package.

// CloneBindVariables returns a shallow-copy of the given bindVariables map.
func CloneBindVariables(bindVariables map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(bindVariables))
	for key, value := range bindVariables {
		result[key] = value
	}
	return result
}

// TruncateQuery all long query strings to a given maximum length to keep logs
// and debug UI output to be a sane length.
func TruncateQuery(query string, max int) string {
	if len(query) <= max {
		return query
	}
	return query[:max-12] + " [TRUNCATED]"
}
