package normalize

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/vtgate/utils"
)

func TestNormalizeAllFields(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	insertQuery := string(`insert into t1 values (1, "chars", "variable chars", x'73757265', 0x676F, 0.33, 9.99, 1, "1976-06-08", "small", "b", "{\"key\":\"value\"}", point(1,5))`)
	normalizedInsertQuery := string(`insert into t1 values (:vtg1, :vtg2, :vtg3, :vtg4, :vtg5, :vtg6, :vtg7, :vtg8, :vtg9, :vtg10, :vtg11, :vtg12, point(:vtg13, :vtg14))`)
	selectQuery := "select * from t1"
	utils.Exec(t, conn, insertQuery)
	qr := utils.Exec(t, conn, selectQuery)
	assert.Equal(t, 1, len(qr.Rows), "wrong number of table rows, expected 1 but had %d. Results: %v", len(qr.Rows), qr.Rows)

	// Now need to figure out the best way to check the normalized query in the planner cache...
	results, err := getPlanCache(fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateProcess.Port))
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

func getPlanCache(vtgateHostPort string) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
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
