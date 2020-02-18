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
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tebeka/selenium"
	"github.com/tebeka/selenium/chrome"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
	"vitess.io/vitess/go/vt/vttest"
)

var (
	localCluster    *vttest.LocalCluster
	cell            = "zone1"
	hostname        = "localhost"
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
	flag.Parse()

	exitcode, err := func() (int, error) {

		tearDownXvfb, err := RunXvfb()
		if err != nil {
			return 1, err
		}
		defer tearDownXvfb()

		// cluster setup
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
		err = CreateWebDriver()
		if err != nil {
			return 1, err
		}
		defer TeardownWebDriver()

		// var cfg vttest.Config
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
func CreateWebDriver() error {
	selenium.SetDebug(true)

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
		return err
	}

	cc := selenium.Capabilities{
		"browserName": "chrome",
	}
	cc.AddChrome(chrome.Capabilities{
		Args: []string{
			"--disable-gpu",
			"--no-sandbox",
			"--headless",
		},
	})

	os.Setenv("webdriver.chrome.driver", os.Getenv("VTROOT")+"/dist")

	var err error
	seleniumService, err = selenium.NewChromeDriverService(os.Getenv("VTROOT")+"/dist/chromedriver/chromedriver", 9515, options)
	if err != nil {
		return err
	}

	wd, err = selenium.NewRemote(cc, "http://localhost:9515/wd/hub")
	if err != nil {
		return err
	}
	name, err := wd.CurrentWindowHandle()
	return wd.ResizeWindow(name, 1280, 1024)
	// return err
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

			headingTxt, err := heading.Text()
			require.Nil(t, err)

			_, err = elem.FindElement(selenium.ByID, headingTxt)
			require.Nil(t, err)

			assert.Contains(t, availableKs, headingTxt)
		}
		return
	}

	assert.Equal(t, 1, len(elems))
	heading, err := elems[0].FindElement(selenium.ByID, "keyspaceName")
	require.Nil(t, err)

	headingTxt, err := heading.Text()
	require.Nil(t, err)

	_, err = elem.FindElement(selenium.ByID, headingTxt)
	require.Nil(t, err)

	assert.Equal(t, selectedKs, headingTxt)
}

func changeDropdownOptions(t *testing.T, dropdownID, dropdownValue string) {
	statusContent, err := wd.FindElement(selenium.ByTagName, "vt-status")
	require.Nil(t, err)

	dropdown, err := statusContent.FindElement(selenium.ByID, dropdownID)
	require.Nil(t, err)

	err = dropdown.Click()
	require.Nil(t, err)
	options, err := dropdown.FindElements(selenium.ByTagName, "li")
	require.Nil(t, err)

	for _, op := range options {
		opTxt, err := op.Text()
		require.Nil(t, err)
		if opTxt == dropdownValue {
			err = op.Click()
			require.Nil(t, err)
			break
		}
	}
}

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

func getDropdownSelection(t *testing.T, group string) string {
	elem, err := wd.FindElement(selenium.ByTagName, "vt-status")
	require.Nil(t, err)
	elem, err = elem.FindElement(selenium.ByID, group)
	require.Nil(t, err)
	elem, err = elem.FindElement(selenium.ByTagName, "label")
	require.Nil(t, err)

	tx, err := elem.Text()
	require.Nil(t, err)
	return tx
}

func getDropdownOptions(t *testing.T, group string) []string {
	elem, err := wd.FindElement(selenium.ByTagName, "vt-status")
	require.Nil(t, err)
	elem, err = elem.FindElement(selenium.ByID, group)
	require.Nil(t, err)
	elems, err := elem.FindElements(selenium.ByTagName, "option")
	require.Nil(t, err)

	var out []string
	for _, elem = range elems {
		tx, err := elem.Text()
		require.Nil(t, err)
		out = append(out, tx)
	}

	return out
}

func getDashboardKeyspaces(t *testing.T) []string {
	wait(t, selenium.ByTagName, "vt-dashboard")

	dashboardContent, err := wd.FindElement(selenium.ByTagName, "vt-dashboard")
	require.Nil(t, err)

	ksCards, err := dashboardContent.FindElements(selenium.ByClassName, "vt-keyspace-card")
	var out []string
	for _, ks := range ksCards {
		txt, err := ks.Text()
		require.Nil(t, err)
		out = append(out, txt)
	}
	return out
}

func getDashboardShards(t *testing.T) []string {
	wait(t, selenium.ByTagName, "vt-dashboard")

	dashboardContent, err := wd.FindElement(selenium.ByTagName, "vt-dashboard")
	require.Nil(t, err)

	ksCards, err := dashboardContent.FindElements(selenium.ByClassName, "vt-shard-stats")
	var out []string
	for _, ks := range ksCards {
		txt, err := ks.Text()
		require.Nil(t, err)
		out = append(out, txt)
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
		txt, err := s.Text()
		require.Nil(t, err)

		out = append(out, txt)
	}
	return out
}

func getShardTablets(t *testing.T) ([]string, []string) {
	wait(t, selenium.ByTagName, "vt-shard-view")
	shardContent, err := wd.FindElement(selenium.ByTagName, "vt-shard-view")
	require.Nil(t, err)

	tableRows, err := shardContent.FindElements(selenium.ByTagName, "tr")
	tableRows = tableRows[1:]

	var tabletTypes, tabletUIDs []string
	for _, row := range tableRows {
		columns, err := row.FindElements(selenium.ByTagName, "td")
		require.Nil(t, err)

		typ, err := columns[1].FindElement(selenium.ByClassName, "ui-cell-data")
		require.Nil(t, err)

		typTxt, err := typ.Text()
		require.Nil(t, err)

		tabletTypes = append(tabletTypes, typTxt)

		uid, err := columns[3].FindElement(selenium.ByClassName, "ui-cell-data")
		require.Nil(t, err)

		uidTxt, err := uid.Text()
		require.Nil(t, err)

		tabletUIDs = append(tabletUIDs, uidTxt)
	}

	return tabletTypes, tabletUIDs
}

// navigation
func navigateToDashBoard(t *testing.T) {
	err := wd.Get(vtctldAddr + "/app2")
	require.Nil(t, err)

	wait(t, selenium.ByID, "test_keyspace")
}

func navigateToKeyspaceView(t *testing.T) {
	navigateToDashBoard(t)
	dashboardContent, err := wd.FindElement(selenium.ByTagName, "vt-dashboard")
	require.Nil(t, err)
	ksCard, err := dashboardContent.FindElements(selenium.ByClassName, "vt-card")
	require.Nil(t, err)
	require.Equal(t, 2, len(ksCard))

	shardStarts, err := ksCard[0].FindElement(selenium.ByTagName, "md-list")
	require.Nil(t, err)

	err = shardStarts.Click()
	require.Nil(t, err)

	wait(t, selenium.ByClassName, "vt-card")
}

func navigateToShardView(t *testing.T) {
	navigateToKeyspaceView(t)
	ksContent, err := wd.FindElement(selenium.ByTagName, "vt-keyspace-view")
	require.Nil(t, err)

	shardCards, err := ksContent.FindElements(selenium.ByClassName, "vt-serving-shard")
	require.Nil(t, err)
	require.Equal(t, 2, len(shardCards))

	err = shardCards[0].Click()
	require.Nil(t, err)

	wait(t, selenium.ByID, "1")
}

func wait(t *testing.T, by, val string) {
	err := wd.WaitWithTimeout(func(xwd selenium.WebDriver) (bool, error) {
		_, err := xwd.FindElement(by, val)
		return err == nil, nil
	}, selenium.DefaultWaitTimeout)
	require.Nil(t, err)
}

func assertDialogCommand(t *testing.T, dialog selenium.WebElement, cmds []string) {
	elms, err := dialog.FindElements(selenium.ByClassName, "vt-sheet")
	require.Nil(t, err)

	var tmpCmd []string
	for _, elm := range elms {
		txt, err := elm.Text()
		require.Nil(t, err)
		tmpCmd = append(tmpCmd, txt)
	}

	assert.ElementsMatch(t, cmds, tmpCmd)
}

func screenshot(t *testing.T, name string) {
	ss, err := wd.Screenshot()
	require.Nil(t, err)
	f, err := os.Create("./" + name)
	require.Nil(t, err)
	_, err = f.Write(ss)
	require.Nil(t, err)
}
