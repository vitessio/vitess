// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package framework

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

// FetchJSON fetches JSON content from the specified URL path and returns it
// as a map. The function returns an empty map on error.
func FetchJSON(urlPath string) map[string]interface{} {
	out := map[string]interface{}{}
	response, err := http.Get(fmt.Sprintf("%s%s", ServerAddress, urlPath))
	if err != nil {
		return out
	}
	defer response.Body.Close()
	_ = json.NewDecoder(response.Body).Decode(&out)
	return out
}

// DebugVars parses /debug/vars and returns a map. The function returns
// an empty map on error.
func DebugVars() map[string]interface{} {
	return FetchJSON("/debug/vars")
}

// FetchInt fetches the specified slash-separated tag and returns the
// value as an int. It returns 0 on error, or if not found.
func FetchInt(vars map[string]interface{}, tags string) int {
	val, _ := FetchVal(vars, tags).(float64)
	return int(val)
}

// FetchVal fetches the specified slash-separated tag and returns the
// value as an interface. It returns nil on error, or if not found.
func FetchVal(vars map[string]interface{}, tags string) interface{} {
	splitTags := strings.Split(tags, "/")
	if len(tags) == 0 {
		return nil
	}
	current := vars
	for _, tag := range splitTags[:len(splitTags)-1] {
		icur, ok := current[tag]
		if !ok {
			return nil
		}
		current, ok = icur.(map[string]interface{})
		if !ok {
			return nil
		}
	}
	return current[splitTags[len(splitTags)-1]]
}

// FetchURL fetches the content from the specified URL path and returns it
// as a string. The function returns an empty string on error.
func FetchURL(urlPath string) string {
	response, err := http.Get(fmt.Sprintf("%s%s", ServerAddress, urlPath))
	if err != nil {
		return ""
	}
	defer response.Body.Close()
	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return ""
	}
	return string(b)
}
