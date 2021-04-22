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
)

func loadMergedPRs(from, to string) ([]string, error) {
	cmd := exec.Command("git", "log", "--oneline", fmt.Sprintf("%s...%s", from, to))
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("%s: %s", err.Error(), string(out))
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

func loadPRInfo(prs []string) ([]prInfo, error) {
	var prInfos []prInfo
	for i, pr := range prs {
		cmd := exec.Command("gh", "pr", "view", pr, "--json", "title,number,labels")
		out, err := cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("%s %s", err.Error(), string(out))
		}
		var prInfo prInfo
		err = json.Unmarshal(out, &prInfo)
		if err != nil {
			return nil, err
		}
		prInfos = append(prInfos, prInfo)
		log.Printf("%d/%d", i, len(prs))
	}
	return prInfos, nil
}

func groupPRs(prInfos []prInfo) map[string]map[string][]prInfo {
	prPerType := map[string]map[string][]prInfo{}

	for _, info := range prInfos {
		var typ, component string
		for _, lbl := range info.Labels {
			switch {
			case strings.HasPrefix(lbl.Name, "Type: "):
				typ = lbl.Name
			case strings.HasPrefix(lbl.Name, "Component: "):
				component = lbl.Name
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

func main() {
	from := flag.String("from", "", "from sha/tag/branch")
	to := flag.String("to", "", "to sha/tag/branch")

	flag.Parse()

	prs, err := loadMergedPRs(*from, *to)
	if err != nil {
		log.Fatal(err)
	}

	prInfos, err := loadPRInfo(prs)
	if err != nil {
		log.Fatal(err)
	}

	prPerType := groupPRs(prInfos)

	t := template.Must(template.New("markdownTemplate").Parse(markdownTemplate))
	err = t.ExecuteTemplate(os.Stdout, "markdownTemplate", prPerType)
	if err != nil {
		log.Fatal(err)
	}
}
