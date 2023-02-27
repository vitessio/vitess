package queryhistory

import "regexp"

func MatchQueries(pattern, query string) (bool, error) {
	if len(pattern) == 0 {
		return len(query) == 0, nil
	}
	if pattern[0] == '/' {
		result, err := regexp.MatchString(pattern[1:], query)
		if err != nil {
			return false, err
		}
		return result, nil
	}
	return (pattern == query), nil
}
