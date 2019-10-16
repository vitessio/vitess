package clustertest

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"testing"
)

func TestVttabletProcess(t *testing.T) {
	testURL(t, "http://localhost:15100/debug/vars/", "tablet debug var url")
	resp, _ := http.Get("http://localhost:15100/debug/vars")
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
