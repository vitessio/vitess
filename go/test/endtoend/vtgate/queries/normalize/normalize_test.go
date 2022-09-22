/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package normalize

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func TestNormalizeAllFields(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	insertQuery := `insert into t1 values (1, "chars", "variable chars", x'73757265', 0x676F, 0.33, 9.99, 1, "1976-06-08", "small", "b", "{\"key\":\"value\"}", point(1,5), b'011', 0b0101)`
	normalizedInsertQuery := `insert into t1 values (:vtg1, :vtg2, :vtg3, :vtg4, :vtg5, :vtg6, :vtg7, :vtg8, :vtg9, :vtg10, :vtg11, :vtg12, point(:vtg13, :vtg14), :vtg15, :vtg16)`
	selectQuery := "select * from t1"
	utils.Exec(t, conn, insertQuery)
	qr := utils.Exec(t, conn, selectQuery)
	assert.Equal(t, 1, len(qr.Rows), "wrong number of table rows, expected 1 but had %d. Results: %v", len(qr.Rows), qr.Rows)

	// Now need to figure out the best way to check the normalized query in the planner cache...
	results, err := getPlanCache(fmt.Sprintf("%s:%d", vtParams.Host, clusterInstance.VtgateProcess.Port))
	require.Nil(t, err)
	found := false
	for _, record := range results {
		key := record["Key"].(string)
		if key == normalizedInsertQuery {
			found = true
			break
		}
	}
	assert.True(t, found, "correctly normalized record not found in planner cache")
}

func getPlanCache(vtgateHostPort string) ([]map[string]any, error) {
	var results []map[string]any
	client := http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := client.Get(fmt.Sprintf("http://%s/debug/query_plans", vtgateHostPort))
	if err != nil {
		return results, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return results, err
	}

	err = json.Unmarshal(body, &results)
	if err != nil {
		return results, err
	}

	return results, nil
}
