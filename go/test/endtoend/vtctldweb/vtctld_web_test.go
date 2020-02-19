/*
Copyright 2020 The Vitess Authors.

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

package vtctldweb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tebeka/selenium"
)

// TestRealtimeStats checks the status by changing dropdown values.
func TestRealtimeStats(t *testing.T) {
	err := wd.Get(vtctldAddr + "/app2")
	require.Nil(t, err)

	statusBtn, err := wd.FindElement(selenium.ByPartialLinkText, "Status")
	require.Nil(t, err)

	err = statusBtn.Click()
	require.Nil(t, err)

	wait(t, selenium.ByTagName, "vt-status")

	testCases := [8][5]string{
		{"", "", "all", "all", "all"},
		{"type", "REPLICA", "all", "all", "REPLICA"},
		{"cell", "test2", "all", "test2", "REPLICA"},
		{"keyspace", "test_keyspace", "test_keyspace", "test2", "REPLICA"},
		{"cell", "all", "test_keyspace", "all", "REPLICA"},
		{"type", "all", "test_keyspace", "all", "all"},
		{"cell", "test2", "test_keyspace", "test2", "all"},
		{"keyspace", "all", "all", "test2", "all"},
	}
	for _, k := range testCases {
		if k[0] != "" && k[1] != "" {
			changeDropdownOptions(t, k[0], k[1])
		}

		tabletOption := []string{"all", "MASTER", "REPLICA", "RDONLY"}
		if k[3] == "test2" {
			tabletOption = []string{"all", "REPLICA", "RDONLY"}
		}

		checkNewView(t, []string{"all", ks1, ks2}, []string{"all", "test", "test2"}, tabletOption, []string{"lag", "qps", "health"}, k[2], k[3], k[4], "health")
	}
}

// TestShardView validates tablet type and uids.
func TestShardView(t *testing.T) {
	navigateToShardView(t)

	tabletTypes, tabletUIDs := getShardTablets(t)

	assert.ElementsMatch(t, []string{"master", "replica", "rdonly", "rdonly", "replica", "replica", "rdonly", "rdonly"}, tabletTypes)
	assert.ElementsMatch(t, []string{"1", "2", "3", "4", "5", "6", "7", "8"}, tabletUIDs)
}

// TestKsView validates the shard names for keyspace.
func TestKsView(t *testing.T) {
	navigateToKeyspaceView(t)
	shards := getKeyspaceShard(t)
	assert.ElementsMatch(t, []string{"-80", "80-"}, shards)
}

// TestCreateKs validates the keyspace creation using ui.
func TestCreateKs(t *testing.T) {
	navigateToDashBoard(t)

	dashboardContent, err := wd.FindElement(selenium.ByTagName, "vt-dashboard")
	require.Nil(t, err)

	dialog, err := dashboardContent.FindElement(selenium.ByTagName, "vt-dialog")
	require.Nil(t, err)

	dashboardMenu, err := dashboardContent.FindElement(selenium.ByClassName, "vt-menu")
	require.Nil(t, err)

	err = dashboardMenu.Click()
	require.Nil(t, err)

	dashboardOptions, err := dashboardContent.FindElements(selenium.ByClassName, "ui-menuitem-text")
	require.Nil(t, err)

	for _, v := range dashboardOptions {
		txt, err := v.Text()
		require.Nil(t, err)

		if txt == "New" {
			err := v.Click()
			require.Nil(t, err)
			break
		}
	}

	inputFields, err := dialog.FindElements(selenium.ByTagName, "md-input")
	require.Nil(t, err)

	for i, input := range inputFields {
		ele, err := input.FindElement(selenium.ByTagName, "input")
		require.Nil(t, err)
		switch i {
		case 0:
			err := ele.SendKeys("test_keyspace3")
			require.Nil(t, err)
			assertDialogCommand(t, dialog, []string{"CreateKeyspace", "-force=false", "test_keyspace3"})

		case 1:
			err := ele.SendKeys("test_id")
			require.Nil(t, err)
			assertDialogCommand(t, dialog, []string{"CreateKeyspace", "-sharding_column_name=test_id", "-sharding_column_type=UINT64", "-force=false", "test_keyspace3"})
		}
	}

	dropdown, err := dialog.FindElement(selenium.ByTagName, "p-dropdown")
	require.Nil(t, err)

	err = dropdown.Click()
	require.Nil(t, err)

	options, err := dropdown.FindElements(selenium.ByTagName, "li")
	require.Nil(t, err)

	err = options[1].Click()
	require.Nil(t, err)

	assertDialogCommand(t, dialog, []string{"CreateKeyspace", "-sharding_column_name=test_id", "-sharding_column_type=BYTES", "-force=false", "test_keyspace3"})

	create, err := dialog.FindElement(selenium.ByID, "vt-action")
	require.Nil(t, err)
	err = create.Click()
	require.Nil(t, err)

	dismiss, err := dialog.FindElement(selenium.ByID, "vt-dismiss")
	require.Nil(t, err)
	err = dismiss.Click()
	require.Nil(t, err)

	ksNames := getDashboardKeyspaces(t)
	assert.ElementsMatch(t, []string{"test_keyspace", "test_keyspace2", "test_keyspace3"}, ksNames)

	testKs, err := dashboardContent.FindElements(selenium.ByClassName, "vt-card")
	require.Nil(t, err)
	menu, err := testKs[2].FindElement(selenium.ByClassName, "vt-menu")
	require.Nil(t, err)
	err = menu.Click()
	require.Nil(t, err)

	options, err = testKs[2].FindElements(selenium.ByTagName, "li")
	require.Nil(t, err)
	for _, v := range options {
		txt, err := v.Text()
		require.Nil(t, err)

		if txt == "Delete" {
			err := v.Click()
			require.Nil(t, err)
			break
		}
	}

	delete, err := dialog.FindElement(selenium.ByID, "vt-action")
	require.Nil(t, err)
	err = delete.Click()
	require.Nil(t, err)

	dismiss, err = dialog.FindElement(selenium.ByID, "vt-dismiss")
	require.Nil(t, err)
	err = dismiss.Click()
	require.Nil(t, err)

	ksNames = getDashboardKeyspaces(t)
	assert.ElementsMatch(t, []string{"test_keyspace", "test_keyspace2"}, ksNames)
}

// TestDashboard validate the keyspaces and shard in dashboard.
func TestDashboard(t *testing.T) {
	navigateToDashBoard(t)
	ksNames := getDashboardKeyspaces(t)
	assert.ElementsMatch(t, []string{"test_keyspace", "test_keyspace2"}, ksNames)
	shardNames := getDashboardShards(t)
	assert.ElementsMatch(t, []string{"2 Shards", "1 Shards"}, shardNames)
}

// TestDashboardValidate validates the validate command from the ui.
func TestDashboardValidate(t *testing.T) {
	navigateToDashBoard(t)
	dashboardContent, err := wd.FindElement(selenium.ByTagName, "vt-dashboard")
	require.Nil(t, err)

	menu, err := dashboardContent.FindElement(selenium.ByClassName, "vt-menu")
	require.Nil(t, err)
	err = menu.Click()
	require.Nil(t, err)

	firstOption, err := dashboardContent.FindElement(selenium.ByClassName, "ui-menuitem-text")
	require.Nil(t, err)
	txt, err := firstOption.Text()
	require.Nil(t, err)
	assert.Equal(t, "Validate", txt)

	err = firstOption.Click()
	require.Nil(t, err)

	dialog, err := dashboardContent.FindElement(selenium.ByTagName, "vt-dialog")
	require.Nil(t, err)

	assertDialogCommand(t, dialog, []string{"Validate", "-ping-tablets=false"})

	checkBoxes, err := dialog.FindElements(selenium.ByClassName, "md-checkbox-inner-container")
	require.Nil(t, err)

	err = checkBoxes[0].Click()
	require.Nil(t, err)

	assertDialogCommand(t, dialog, []string{"Validate", "-ping-tablets"})

	validate, err := dialog.FindElement(selenium.ByID, "vt-action")
	require.Nil(t, err)
	err = validate.Click()
	require.Nil(t, err)
	validateResp, err := dialog.FindElement(selenium.ByClassName, "vt-resp")
	require.Nil(t, err)
	txt, err = validateResp.Text()
	require.Nil(t, err)

	fmt.Printf("Validate command response: %s\n", txt)

	dismiss, err := dialog.FindElement(selenium.ByID, "vt-dismiss")
	require.Nil(t, err)
	err = dismiss.Click()
	require.Nil(t, err)
}
