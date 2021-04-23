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

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"sync"
	"text/template"
)

type label struct {
	Name string `json:"name"`
}

type prInfo struct {
	Labels []label `json:"labels"`
	Number int     `json:"number"`
	Title  string  `json:"title"`
}

const (
	markdownTemplate = `
{{- range $typeName, $components := . }}
## {{ $typeName }}
{{- range $componentName, $component := $components }} 
### {{ $componentName}}
{{- range $prInfo := $component }}
 - {{ $prInfo.Title }} #{{ $prInfo.Number }}
{{- end }}
{{- end }}
{{- end }}
`

	prefixType      = "Type: "
	prefixComponent = "Component: "
)

func loadMergedPRs(from, to string) ([]string, error) {
	cmd := exec.Command("git", "log", "--oneline", fmt.Sprintf("%s..%s", from, to))
	out, err := cmd.Output()
	if err != nil {
		execErr := err.(*exec.ExitError)
		return nil, fmt.Errorf("%s:\nstderr: %s\nstdout: %s", err.Error(), execErr.Stderr, out)
	}

	var prs []string
	rgx := regexp.MustCompile(`Merge pull request #(\d+)`)
	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		lineInfo := rgx.FindStringSubmatch(line)
		if len(lineInfo) == 2 {
			prs = append(prs, lineInfo[1])
		}
	}

	sort.Strings(prs)
	return prs, nil
}

func loadPRinfo(pr string) (prInfo, error) {
	cmd := exec.Command("gh", "pr", "view", pr, "--json", "title,number,labels")
	out, err := cmd.Output()
	if err != nil {
		execErr := err.(*exec.ExitError)
		return prInfo{}, fmt.Errorf("%s:\nstderr: %s\nstdout: %s", err.Error(), execErr.Stderr, out)
	}
	var prInfo prInfo
	err = json.Unmarshal(out, &prInfo)
	return prInfo, err
}

func loadAllPRs(prs []string) ([]prInfo, error) {
	errChan := make(chan error)
	wgDone := make(chan bool)
	prChan := make(chan string, len(prs))
	// fill the work queue
	for _, s := range prs {
		prChan <- s
	}
	close(prChan)

	var prInfos []prInfo

	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			// load meta data about PRs
			defer wg.Done()
			for b := range prChan {
				fmt.Print(".")
				prInfo, err := loadPRinfo(b)
				if err != nil {
					errChan <- err
					break
				}
				mu.Lock()
				prInfos = append(prInfos, prInfo)
				mu.Unlock()
			}
		}()
	}

	go func() {
		// wait for the loading to finish
		wg.Wait()
		close(wgDone)
	}()

	var err error
	select {
	case <-wgDone:
		break
	case err = <-errChan:
		break
	}

	fmt.Println()
	return prInfos, err
}

func groupPRs(prInfos []prInfo) map[string]map[string][]prInfo {
	prPerType := map[string]map[string][]prInfo{}

	for _, info := range prInfos {
		var typ, component string
		for _, lbl := range info.Labels {
			switch {
			case strings.HasPrefix(lbl.Name, prefixType):
				typ = strings.TrimPrefix(lbl.Name, prefixType)
			case strings.HasPrefix(lbl.Name, prefixComponent):
				component = strings.TrimPrefix(lbl.Name, prefixComponent)
			}
		}
		if typ == "" {
			typ = "Other"
		}
		if component == "" {
			component = "Other"
		}
		components, exists := prPerType[typ]
		if !exists {
			components = map[string][]prInfo{}
			prPerType[typ] = components
		}

		prsPerComponentAndType := components[component]
		components[component] = append(prsPerComponentAndType, info)
	}
	return prPerType
}

func writePrInfos(fileout string, prPerType map[string]map[string][]prInfo) (err error) {
	writeTo := os.Stdout
	if fileout != "" {
		writeTo, err = os.OpenFile(fileout, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return err
		}
	}

	t := template.Must(template.New("markdownTemplate").Parse(markdownTemplate))
	err = t.ExecuteTemplate(writeTo, "markdownTemplate", prPerType)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	from := flag.String("from", "", "from sha/tag/branch")
	to := flag.String("to", "HEAD", "to sha/tag/branch")
	fileout := flag.String("file", "", "file on which to write release notes, stdout if empty")

	flag.Parse()

	prs, err := loadMergedPRs(*from, *to)
	if err != nil {
		log.Fatal(err)
	}

	prInfos, err := loadAllPRs(prs)
	if err != nil {
		log.Fatal(err)
	}

	prPerType := groupPRs(prInfos)

	err = writePrInfos(*fileout, prPerType)
	if err != nil {
		log.Fatal(err)
	}
}
