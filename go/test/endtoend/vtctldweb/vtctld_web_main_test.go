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
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tebeka/selenium"
	"github.com/tebeka/selenium/chrome"

	"vitess.io/vitess/go/test/endtoend/cluster"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
	"vitess.io/vitess/go/vt/vttest"
)

//nolint
var (
	localCluster    *vttest.LocalCluster
	hostname        = "localhost" //nolint
	wd              selenium.WebDriver
	seleniumService *selenium.Service
	vtctldAddr      string
	ks1             = "test_keyspace"
	ks2             = "test_keyspace2"
	sqlSchema       = "CREATE TABLE test_table (\n" +
		"	`id` BIGINT(20) UNSIGNED NOT NULL,\n" +
		"	`msg` VARCHAR(64),\n" +
		"	`keyspace_id` BIGINT(20) UNSIGNED NOT NULL,\n" +
		"	PRIMARY KEY (id)\n" +
		") ENGINE=InnoDB"
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitcode, err := func() (int, error) {

		// runs Xvfb in background
		tearDownXvfb, err := RunXvfb()
		if err != nil {
			return 1, err
		}
		defer tearDownXvfb()

		// cluster setup using vtcombo
		topology := new(vttestpb.VTTestTopology)
		topology.Cells = []string{"test", "test2"}
		topology.Keyspaces = []*vttestpb.Keyspace{
			{
				Name: ks1,
				Shards: []*vttestpb.Shard{
					{Name: "-80"},
					{Name: "80-"},
				},
				RdonlyCount:  2,
				ReplicaCount: 2,
			},
			{
				Name: ks2,
				Shards: []*vttestpb.Shard{
					{Name: "0"},
				},
				RdonlyCount:  2,
				ReplicaCount: 1,
			},
		}

		// create driver here
		err = CreateWebDriver(getPort())
		if err != nil {
			return 1, err
		}
		defer TeardownWebDriver()

		var cfg vttest.Config
		cfg.Topology = topology
		cfg.SchemaDir = os.Getenv("VTROOT") + "/test/vttest_schema"
		cfg.DefaultSchemaDir = os.Getenv("VTROOT") + "/test/vttest_schema/default"

		localCluster = &vttest.LocalCluster{
			Config: cfg,
		}

		err = localCluster.Setup()
		defer localCluster.TearDown()

		vtctldAddr = fmt.Sprintf("http://localhost:%d", localCluster.Env.PortForProtocol("vtcombo", "port"))
		if err != nil {
			return 1, err
		}

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}
}

// RunXvfb runs Xvfb command in background and returns the teardown function.
func RunXvfb() (func() error, error) {

	tmpProcess := exec.Command("Xvfb", ":15", "-ac")

	err := tmpProcess.Start()
	if err != nil {
		return nil, err
	}

	exit := make(chan error)
	go func() {
		exit <- tmpProcess.Wait()
	}()

	teardownFunc := func() error {
		tmpProcess.Process.Signal(syscall.SIGTERM)
		select {
		case <-exit:
			return nil
		case <-time.After(10 * time.Second):
			tmpProcess.Process.Kill()
			return <-exit
		}
	}

	os.Setenv("DISPLAY", ":15")

	return teardownFunc, nil
}

// CreateWebDriver Creates a webdriver object (local or remote for Travis).
func CreateWebDriver(port int) error {
	// selenium.SetDebug(true)

	// Set common Options
	options := selenium.ChromeDriver(os.Getenv("VTROOT") + "/dist")

	if os.Getenv("CI") == "true" && os.Getenv("TRAVIS") == "true" {

		capabilities := selenium.Capabilities{}
		capabilities["tunnel-identifier"] = os.Getenv("TRAVIS_JOB_NUMBER")
		capabilities["build"] = os.Getenv("TRAVIS_BUILD_NUMBER")
		capabilities["platform"] = "Linux"
		capabilities["browserName"] = "chrome"
		capabilities["chromeOptions"] = options

		var err error
		wd, err = selenium.NewRemote(capabilities, fmt.Sprintf("%s:%s@localhost:4445/wd/hub", os.Getenv("SAUCE_USERNAME"), os.Getenv("SAUCE_ACCESS_KEY")))
		if err != nil {
			return err
		}

		name, err := wd.CurrentWindowHandle() //nolint
		return wd.ResizeWindow(name, 1280, 1024)
	}

	// Only testing against Chrome for now
	cc := selenium.Capabilities{"browserName": "chrome"}
	cc.AddChrome(chrome.Capabilities{
		Args: []string{
			"--disable-gpu",
			"--no-sandbox",
			"--headless",
		},
	})

	os.Setenv("webdriver.chrome.driver", os.Getenv("VTROOT")+"/dist")

	var err error
	seleniumService, err = selenium.NewChromeDriverService(os.Getenv("VTROOT")+"/dist/chromedriver/chromedriver", port, options)
	if err != nil {
		return err
	}

	wd, err = selenium.NewRemote(cc, fmt.Sprintf("http://localhost:%d/wd/hub", port))
	if err != nil {
		return err
	}
	name, err := wd.CurrentWindowHandle() //nolint
	return wd.ResizeWindow(name, 1280, 1024)
}

func TeardownWebDriver() {
	wd.Quit()
	if seleniumService != nil {
		seleniumService.Stop()

	}
}

func checkNewView(t *testing.T, keyspaces, cells, types, metrics []string, selectedKs, selectedCell, selectedType, selectedMetric string) {
	checkDropdowns(t, keyspaces, cells, types, metrics, selectedKs, selectedCell, selectedType, selectedMetric)
	checkHeatMaps(t, selectedKs)
}

func checkHeatMaps(t *testing.T, selectedKs string) {
	elem, err := wd.FindElement(selenium.ByTagName, "vt-status")
	require.Nil(t, err)

	elems, err := elem.FindElements(selenium.ByTagName, "vt-heatmap")
	require.Nil(t, err)

	if selectedKs == "all" {
		availableKs := getDropdownOptions(t, "keyspace")
		assert.Equal(t, len(elems), len(availableKs)-1)
		for _, elem := range elems {
			heading, err := elem.FindElement(selenium.ByID, "keyspaceName")
			require.Nil(t, err)

			headingTxt := text(t, heading)

			_, err = elem.FindElement(selenium.ByID, headingTxt)
			require.Nil(t, err)

			assert.Contains(t, availableKs, headingTxt)
		}
		return
	}

	assert.Equal(t, 1, len(elems))
	heading, err := elems[0].FindElement(selenium.ByID, "keyspaceName")
	require.Nil(t, err)

	headingTxt := text(t, heading)

	_, err = elem.FindElement(selenium.ByID, headingTxt)
	require.Nil(t, err)

	assert.Equal(t, selectedKs, headingTxt)
}

// changeDropdownOptions changes the selected value of dropdown.
func changeDropdownOptions(t *testing.T, dropdownID, dropdownValue string) {
	statusContent, err := wd.FindElement(selenium.ByTagName, "vt-status")
	require.Nil(t, err)

	dropdown, err := statusContent.FindElement(selenium.ByID, dropdownID)
	require.Nil(t, err)

	click(t, dropdown)
	options, err := dropdown.FindElements(selenium.ByTagName, "li")
	require.Nil(t, err)

	triedOption := []string{}
	for _, op := range options {
		opTxt := text(t, op)
		if opTxt == dropdownValue {
			click(t, op)
			return
		}

		triedOption = append(triedOption, opTxt)
	}
	ss(t, "option_check")
	t.Log("dropdown options change failed", strings.Join(triedOption, ","), dropdownValue)
}

// checkDropdowns validates the dropdown values and selected value.
func checkDropdowns(t *testing.T, keyspaces, cells, types, metrics []string, selectedKs, selectedCell, selectedType, selectedMetric string) {

	Options := getDropdownOptions(t, "keyspace")
	Selected := getDropdownSelection(t, "keyspace")

	assert.Equal(t, keyspaces, Options)
	assert.Equal(t, selectedKs, Selected)

	Options = getDropdownOptions(t, "cell")
	Selected = getDropdownSelection(t, "cell")

	assert.Equal(t, cells, Options)
	assert.Equal(t, selectedCell, Selected)

	Options = getDropdownOptions(t, "type")
	Selected = getDropdownSelection(t, "type")

	assert.Equal(t, types, Options)
	assert.Equal(t, selectedType, Selected)

	Options = getDropdownOptions(t, "metric")
	Selected = getDropdownSelection(t, "metric")

	assert.Equal(t, metrics, Options)
	assert.Equal(t, selectedMetric, Selected)

}

// get element functions
// getDropdownSelection fetchs selected value for corresponding group.
func getDropdownSelection(t *testing.T, group string) string {
	elem, err := wd.FindElement(selenium.ByTagName, "vt-status")
	require.Nil(t, err)
	elem, err = elem.FindElement(selenium.ByID, group)
	require.Nil(t, err)
	elem, err = elem.FindElement(selenium.ByTagName, "label")
	require.Nil(t, err)

	return text(t, elem)
}

// getDropdownOptions fetchs list of option available for corresponding group.
func getDropdownOptions(t *testing.T, group string) []string {
	elem, err := wd.FindElement(selenium.ByTagName, "vt-status")
	require.Nil(t, err)
	elem, err = elem.FindElement(selenium.ByID, group)
	require.Nil(t, err)
	elems, err := elem.FindElements(selenium.ByTagName, "option")
	require.Nil(t, err)

	var out []string
	for _, elem = range elems {
		out = append(out, text(t, elem))
	}

	return out
}

// getDashboardKeyspaces fetches keyspaces from the dashboard.
func getDashboardKeyspaces(t *testing.T) []string {
	wait(t, selenium.ByTagName, "vt-dashboard")

	dashboardContent, err := wd.FindElement(selenium.ByTagName, "vt-dashboard")
	require.Nil(t, err)

	ksCards, err := dashboardContent.FindElements(selenium.ByClassName, "vt-keyspace-card") //nolint
	var out []string
	for _, ks := range ksCards {
		out = append(out, text(t, ks))
	}
	return out
}

// getDashboardShards fetches shards from the dashboard.
func getDashboardShards(t *testing.T) []string {
	wait(t, selenium.ByTagName, "vt-dashboard")

	dashboardContent, err := wd.FindElement(selenium.ByTagName, "vt-dashboard") //nolint
	require.Nil(t, err)

	ksCards, err := dashboardContent.FindElements(selenium.ByClassName, "vt-shard-stats") //nolint
	var out []string
	for _, ks := range ksCards {
		out = append(out, text(t, ks))
	}
	return out
}

func getKeyspaceShard(t *testing.T) []string {
	wait(t, selenium.ByTagName, "vt-keyspace-view")

	ksContent, err := wd.FindElement(selenium.ByTagName, "vt-keyspace-view")
	require.Nil(t, err)

	shards, err := ksContent.FindElements(selenium.ByClassName, "vt-serving-shard")
	require.Nil(t, err)
	var out []string
	for _, s := range shards {
		out = append(out, text(t, s))
	}
	return out
}

// getShardTablets gives list of tablet type and uid.
func getShardTablets(t *testing.T) ([]string, []string) {
	wait(t, selenium.ByTagName, "vt-shard-view")
	shardContent, err := wd.FindElement(selenium.ByTagName, "vt-shard-view")
	require.Nil(t, err)

	tableRows, err := shardContent.FindElements(selenium.ByTagName, "tr") //nolint
	tableRows = tableRows[1:]

	var tabletTypes, tabletUIDs []string
	for _, row := range tableRows {
		columns, err := row.FindElements(selenium.ByTagName, "td")
		require.Nil(t, err)

		typ, err := columns[1].FindElement(selenium.ByClassName, "ui-cell-data")
		require.Nil(t, err)

		typTxt := text(t, typ)

		tabletTypes = append(tabletTypes, typTxt)

		uid, err := columns[3].FindElement(selenium.ByClassName, "ui-cell-data")
		require.Nil(t, err)

		uidTxt := text(t, uid)
		tabletUIDs = append(tabletUIDs, uidTxt)
	}

	return tabletTypes, tabletUIDs
}

// navigation functions
// navigateToDashBoard navigates chrome screen to dashboard of vitess.
func navigateToDashBoard(t *testing.T) {
	err := wd.Get(vtctldAddr + "/app2")
	require.Nil(t, err)

	wait(t, selenium.ByID, "test_keyspace")
}

// navigateToKeyspaceView navigates chrome screen to first keyspace.
func navigateToKeyspaceView(t *testing.T) {
	navigateToDashBoard(t)
	dashboardContent, err := wd.FindElement(selenium.ByTagName, "vt-dashboard")
	require.Nil(t, err)
	ksCard, err := dashboardContent.FindElements(selenium.ByClassName, "vt-card")
	require.Nil(t, err)
	require.Equal(t, 2, len(ksCard))

	shardStarts, err := ksCard[0].FindElement(selenium.ByTagName, "md-list")
	require.Nil(t, err)

	click(t, shardStarts)

	wait(t, selenium.ByClassName, "vt-card")
}

// navigateToShardView navigates chrome screen to the first shard of first keyspace.
func navigateToShardView(t *testing.T) {
	navigateToKeyspaceView(t)
	ksContent, err := wd.FindElement(selenium.ByTagName, "vt-keyspace-view")
	require.Nil(t, err)

	shardCards, err := ksContent.FindElements(selenium.ByClassName, "vt-serving-shard")
	require.Nil(t, err)
	require.Equal(t, 2, len(shardCards))

	click(t, shardCards[0])

	wait(t, selenium.ByID, "1")
}

// other utility
// wait waits for the given element to be discoverable.
func wait(t *testing.T, by, val string) {
	err := wd.WaitWithTimeout(func(xwd selenium.WebDriver) (bool, error) {
		_, err := xwd.FindElement(by, val)
		return err == nil, nil
	}, selenium.DefaultWaitTimeout)
	require.Nil(t, err)
}

// assertDialogCommand validates the command in dialog.
func assertDialogCommand(t *testing.T, dialog selenium.WebElement, cmds []string) {
	elms, err := dialog.FindElements(selenium.ByClassName, "vt-sheet")
	require.Nil(t, err)

	var tmpCmd []string
	for _, elm := range elms {
		tmpCmd = append(tmpCmd, text(t, elm))
	}

	assert.ElementsMatch(t, cmds, tmpCmd)
}

func text(t *testing.T, elem selenium.WebElement) string {
	for i := 0; i < 5; i++ {
		opTxt, err := elem.Text()
		require.Nil(t, err)
		if opTxt != "" {
			return opTxt
		}
	}

	return ""
}

func click(t *testing.T, elem selenium.WebElement) {
	require.Nil(t, elem.Click())
}

// ss takes screenshot of chrome, for debugging only.
func ss(t *testing.T, name string) {
	b, err := wd.Screenshot()
	require.Nil(t, err)
	f, err := os.Create("./" + name)
	require.Nil(t, err)
	_, err = f.Write(b)
	require.Nil(t, err)
}

func getPort() int {
	return 20000 + rand.Intn(10000)
}
