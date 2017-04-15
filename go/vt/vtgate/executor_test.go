package vtgate

import (
	"bytes"
	"html/template"
	"reflect"
	"strings"
	"testing"
)

// TestVSchemaStats makes sure the building and displaying of the
// VSchemaStats works.
func TestVSchemaStats(t *testing.T) {
	r, _, _, _ := createExecutorEnv()

	stats := r.VSchemaStats()

	templ := template.New("")
	templ, err := templ.Parse(VSchemaTemplate)
	if err != nil {
		t.Fatalf("error parsing template: %v", err)
	}
	wr := &bytes.Buffer{}
	if err := templ.Execute(wr, stats); err != nil {
		t.Fatalf("error executing template: %v", err)
	}
	result := wr.String()
	if !strings.Contains(result, "<td>TestBadSharding</td>") ||
		!strings.Contains(result, "<td>TestUnsharded</td>") {
		t.Errorf("invalid html result: %v", result)
	}
}

func TestGetPlanUnnormalized(t *testing.T) {
	r, _, _, _ := createExecutorEnv()
	query1 := "select * from music_user_map where id = 1"
	plan1, err := r.getPlan(query1, "", map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	plan2, err := r.getPlan(query1, "", map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	if plan1 != plan2 {
		t.Errorf("getPlan(query1): plans must be equal: %p %p", plan1, plan2)
	}
	want := []string{
		query1,
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}
	plan3, err := r.getPlan(query1, KsTestUnsharded, map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	if plan1 == plan3 {
		t.Errorf("getPlan(query1, ks): plans must not be equal: %p %p", plan1, plan3)
	}
	plan4, err := r.getPlan(query1, KsTestUnsharded, map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	if plan3 != plan4 {
		t.Errorf("getPlan(query1, ks): plans must be equal: %p %p", plan3, plan4)
	}
	want = []string{
		KsTestUnsharded + ":" + query1,
		query1,
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}
}

func TestGetPlanNormalized(t *testing.T) {
	r, _, _, _ := createExecutorEnv()
	r.normalize = true
	query1 := "select * from music_user_map where id = 1"
	query2 := "select * from music_user_map where id = 2"
	normalized := "select * from music_user_map where id = :vtg1"
	plan1, err := r.getPlan(query1, "", map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	plan2, err := r.getPlan(query1, "", map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	if plan1 != plan2 {
		t.Errorf("getPlan(query1): plans must be equal: %p %p", plan1, plan2)
	}
	want := []string{
		normalized,
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}
	plan3, err := r.getPlan(query2, "", map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	if plan1 != plan3 {
		t.Errorf("getPlan(query2): plans must be equal: %p %p", plan1, plan3)
	}
	plan4, err := r.getPlan(normalized, "", map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	if plan1 != plan4 {
		t.Errorf("getPlan(normalized): plans must be equal: %p %p", plan1, plan4)
	}

	plan3, err = r.getPlan(query1, KsTestUnsharded, map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	if plan1 == plan3 {
		t.Errorf("getPlan(query1, ks): plans must not be equal: %p %p", plan1, plan3)
	}
	plan4, err = r.getPlan(query1, KsTestUnsharded, map[string]interface{}{})
	if err != nil {
		t.Error(err)
	}
	if plan3 != plan4 {
		t.Errorf("getPlan(query1, ks): plans must be equal: %p %p", plan3, plan4)
	}
	want = []string{
		KsTestUnsharded + ":" + normalized,
		normalized,
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}

	// Errors
	_, err = r.getPlan("syntax", "", map[string]interface{}{})
	wantErr := "syntax error at position 7 near 'syntax'"
	if err == nil || err.Error() != wantErr {
		t.Errorf("getPlan(syntax): %v, want %s", err, wantErr)
	}
	_, err = r.getPlan("create table a(id int)", "", map[string]interface{}{})
	wantErr = "unsupported construct: ddl"
	if err == nil || err.Error() != wantErr {
		t.Errorf("getPlan(syntax): %v, want %s", err, wantErr)
	}
	if keys := r.plans.Keys(); !reflect.DeepEqual(keys, want) {
		t.Errorf("Plan keys: %s, want %s", keys, want)
	}
}
