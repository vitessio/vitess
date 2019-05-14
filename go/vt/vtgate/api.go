/*
Copyright 2018 GitHub Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtgate

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
)

// This file implements a REST-style API for the vtgate web interface.

const (
	apiPrefix = "/api/"

	jsonContentType = "application/json; charset=utf-8"
)

func httpErrorf(ctx context.Context, w http.ResponseWriter, r *http.Request, format string, args ...interface{}) {
	errMsg := fmt.Sprintf(format, args...)
	log.ErrorfC(ctx, "HTTP error on %v: %v, request: %#v", r.URL.Path, errMsg, r)
	http.Error(w, errMsg, http.StatusInternalServerError)
}

func handleAPI(ctx context.Context, apiPath string, handlerFunc func(w http.ResponseWriter, r *http.Request) error) {
	http.HandleFunc(apiPrefix+apiPath, func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if x := recover(); x != nil {
				httpErrorf(ctx, w, r, "uncaught panic: %v", x)
			}
		}()
		if err := handlerFunc(w, r); err != nil {
			httpErrorf(ctx, w, r, "%v", err)
		}
	})
}

func handleCollection(ctx context.Context, collection string, getFunc func(*http.Request) (interface{}, error)) {
	handleAPI(ctx, collection+"/", func(w http.ResponseWriter, r *http.Request) error {
		// Get the requested object.
		obj, err := getFunc(r)
		if err != nil {
			return fmt.Errorf("can't get %v: %v", collection, err)
		}

		// JSON encode response.
		data, err := json.MarshalIndent(obj, "", "  ")
		if err != nil {
			return fmt.Errorf("cannot marshal data: %v", err)
		}
		w.Header().Set("Content-Type", jsonContentType)
		w.Write(data)
		return nil
	})
}

func getItemPath(url string) string {
	// Strip API prefix.
	if !strings.HasPrefix(url, apiPrefix) {
		return ""
	}
	url = url[len(apiPrefix):]

	// Strip collection name.
	parts := strings.SplitN(url, "/", 2)
	if len(parts) != 2 {
		return ""
	}
	return parts[1]
}

func initAPI(ctx context.Context, hc discovery.HealthCheck) {
	// Healthcheck real time status per (cell, keyspace, tablet type, metric).
	handleCollection(ctx, "health-check", func(r *http.Request) (interface{}, error) {
		cacheStatus := hc.CacheStatus()

		itemPath := getItemPath(r.URL.Path)
		if itemPath == "" {
			return cacheStatus, nil
		}
		parts := strings.SplitN(itemPath, "/", 2)
		collectionFilter := parts[0]
		if collectionFilter == "" {
			return cacheStatus, nil
		}
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid health-check path: %q  expected path: / or /cell/<cell> or /keyspace/<keyspace> or /tablet/<tablet|mysql_hostname>", itemPath)
		}
		value := parts[1]

		switch collectionFilter {
		case "cell":
			{
				filteredStatus := make(discovery.TabletsCacheStatusList, 0)
				for _, tabletCacheStatus := range cacheStatus {
					if tabletCacheStatus.Cell == value {
						filteredStatus = append(filteredStatus, tabletCacheStatus)
					}
				}
				return filteredStatus, nil
			}
		case "keyspace":
			{
				filteredStatus := make(discovery.TabletsCacheStatusList, 0)
				for _, tabletCacheStatus := range cacheStatus {
					if tabletCacheStatus.Target.Keyspace == value {
						filteredStatus = append(filteredStatus, tabletCacheStatus)
					}
				}
				return filteredStatus, nil
			}
		case "tablet":
			{
				// Return a _specific tablet_
				for _, tabletCacheStatus := range cacheStatus {
					for _, tabletStats := range tabletCacheStatus.TabletsStats {
						if tabletStats.Name == value || tabletStats.Tablet.MysqlHostname == value {
							return tabletStats, nil
						}
					}
				}
			}
		}
		return nil, fmt.Errorf("cannot find health for: %s", itemPath)
	})
}
