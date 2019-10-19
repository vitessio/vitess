package clustertest

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
)

func TestVttabletProcess(t *testing.T) {
	firstTabletPort := ClusterInstance.Keyspaces[0].Shards[0].Vttablets[0].HTTPPort
	testURL(t, fmt.Sprintf("http://localhost:%d/debug/vars/", firstTabletPort), "tablet debug var url")
	resp, _ := http.Get(fmt.Sprintf("http://localhost:%d/debug/vars", firstTabletPort))
	resultMap := make(map[string]interface{})
	respByte, _ := ioutil.ReadAll(resp.Body)
	err := json.Unmarshal(respByte, &resultMap)
	if err != nil {
		panic(err)
	}
	if got, want := resultMap["TabletKeyspace"], "commerce"; got != want {
		t.Errorf("select:\n%v want\n%v for %s", got, want, "Keyspace of tablet should match")
	}
}
