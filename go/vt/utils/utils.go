package utils

// utils.go contains general utility functions used in the splitquery package.

// cloneBindVariables returns a shallow-copy of the given bindVariables map.
func CloneBindVariables(bindVariables map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(bindVariables))
	for key, value := range bindVariables {
		result[key] = value
	}
	return result
}
