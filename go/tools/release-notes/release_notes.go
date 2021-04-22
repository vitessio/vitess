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

func main() {
	//dir := flag.Strigng("dir", "", "the project directory")
	from := flag.String("from", "", "from sha/tag/branch")
	to := flag.String("to", "", "to sha/tag/branch")

	flag.Parse()
	//❯ git log --oneline release-10...mster | grep 'Merge pull request #'
	// 42af3c1000 Merge pull request #7909 from enisoc/enisoc-email
	// fetch merge PR commits
	cmd := exec.Command("git", "log", "--oneline", fmt.Sprintf("%s...%s", *from, *to))
	out, err := cmd.Output()
	if err != nil {
		log.Fatalf("%s %s", err.Error(), string(out))
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

	var prInfos []prInfo
	for i, pr := range prs {
		cmd := exec.Command("gh", "pr", "view", pr, "--json", "title,number,labels")
		out, err := cmd.Output()
		if err != nil {
			log.Fatalf("%s %s", err.Error(), string(out))
		}
		var prInfo prInfo
		err = json.Unmarshal(out, &prInfo)
		if err != nil {
			log.Fatal(err)
		}
		prInfos = append(prInfos, prInfo)
		log.Printf("%d/%d", i, len(prs))
	}

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

	t := template.Must(template.New("markdownTemplate").Parse(markdownTemplate))
	err = t.ExecuteTemplate(os.Stdout, "markdownTemplate", prPerType)
	if err != nil {
		log.Fatal(err)
	}
	/*
	   {
	     "labels": [
	       {
	         "name": "dependencies"
	       },
	       {
	         "name": "javascript"
	       }
	     ],
	     "number": 7920,
	     "title": "Bump sockjs from 0.3.19 to 0.3.21 in /web/vtctld2"
	   }
	*/
	// foreach PR gh pr view 7920 --json title,number,labels
	//	fetch json info -> storage

	// group by/sort/template out
}
