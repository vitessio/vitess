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
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/collations"
)

func wikiRequest(lang string, args map[string]string, output interface{}) error {
	wikipedia := fmt.Sprintf("https://%s.wikipedia.org/w/api.php", lang)
	req, err := http.NewRequest("GET", wikipedia, nil)
	if err != nil {
		return err
	}

	q := url.Values{}
	for k, v := range args {
		q.Add(k, v)
	}

	req.URL.RawQuery = q.Encode()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("status code: %d", resp.StatusCode)
	}

	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(output); err != nil {
		return err
	}
	return nil
}

func getTextFromWikipedia(lang string, article string) (string, error) {
	const MaxChars = 750
	options := map[string]string{
		"action":        "query",
		"format":        "json",
		"prop":          "extracts",
		"titles":        article,
		"formatversion": "2",
		"exchars":       fmt.Sprintf("%d", MaxChars),
		// "exsentences":     "5",
		"explaintext":     "1",
		"exsectionformat": "plain",
	}

	var response struct {
		Query struct {
			Pages []struct {
				Title   string `json:"titles"`
				Extract string `json:"extract"`
			} `json:"pages"`
		} `json:"query"`
	}

	if err := wikiRequest(lang, options, &response); err != nil {
		return "", err
	}

	var chunks []string
	for _, page := range response.Query.Pages {
		chunks = append(chunks, page.Extract)
	}
	return strings.Join(chunks, "\n"), nil
}

func getAllLanguages(article string) (map[string]string, error) {
	allLanguages := make(map[string]string)
	options := map[string]string{
		"action": "query",
		"format": "json",
		"prop":   "langlinks",
		"titles": article,
		"limit":  "100",
	}

	for {
		var response struct {
			Continue map[string]string
			Query    struct {
				Pages map[string]struct {
					Title     string `json:"titles"`
					LangLinks []struct {
						Lang string `json:"lang"`
						Path string `json:"*"`
					} `json:"langlinks"`
				} `json:"pages"`
			} `json:"query"`
		}

		if err := wikiRequest("en", options, &response); err != nil {
			return nil, err
		}

		if len(response.Query.Pages) != 1 {
			return nil, fmt.Errorf("expected 1 page returned, got %d", len(response.Query.Pages))
		}

		for _, firstPage := range response.Query.Pages {
			for _, langlink := range firstPage.LangLinks {
				allLanguages[langlink.Lang] = langlink.Path
			}
		}

		if len(response.Continue) == 0 {
			break
		}

		for k, v := range response.Continue {
			options[k] = v
		}
	}
	return allLanguages, nil
}

var wantLanguages = map[string]bool{
	"ja": true,
	"vi": true,
	"zh": true,
	"ru": true,
	"hr": true,
	"hu": true,
	"eo": true,
	"la": true,
	"es": true,
	"sk": true,
	"lt": true,
	"da": true,
	"cs": true,
	"tr": true,
	"sv": true,
	"et": true,
	"pl": true,
	"sl": true,
	"ro": true,
	"lv": true,
	"is": true,
	"de": true,
}

func colldump(colldumpPath, collation string, input []byte) []byte {
	cmd := exec.Command(colldumpPath, "--test", collation)
	cmd.Stdin = bytes.NewReader(input)
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

func main() {
	colldumpPath, err := exec.LookPath("colldump")
	if err != nil {
		log.Fatal(err)
	}

	var allcoll []collations.CollationUCA
	for _, collation := range collations.All() {
		if tc, ok := collation.(collations.CollationUCA); ok {
			allcoll = append(allcoll, tc)
		}
	}

	articles, err := getAllLanguages(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	var tdata = &collations.GoldenTest{Name: os.Args[1]}

	for lang, article := range articles {
		if !wantLanguages[lang] {
			continue
		}
		start := time.Now()
		log.Printf("[%s] %q", lang, article)
		snippet, err := getTextFromWikipedia(lang, article)
		if err != nil {
			log.Printf("error: %v", err)
			continue
		}
		log.Printf("[%s] %v", lang, time.Since(start))

		gcase := collations.GoldenCase{
			Lang:    lang,
			Text:    []byte(snippet),
			Weights: make(map[string][]byte),
		}
		for _, collation := range allcoll {
			transcoded, err := collation.Encoding().EncodeFromUTF8([]byte(snippet))
			if err != nil {
				log.Printf("[%s] skip collation %s", lang, collation.Name())
				continue
			}

			weights := colldump(colldumpPath, collation.Name(), transcoded)
			gcase.Weights[collation.Name()] = weights
		}

		tdata.Cases = append(tdata.Cases, gcase)
	}

	if err := tdata.EncodeToFile(fmt.Sprintf("testdata/wiki_%x.gob.gz", os.Args[1])); err != nil {
		log.Fatal(err)
	}
}
