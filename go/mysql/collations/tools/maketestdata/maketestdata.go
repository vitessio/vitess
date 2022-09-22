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
	"vitess.io/vitess/go/mysql/collations/internal/charset"
	"vitess.io/vitess/go/mysql/collations/internal/testutil"
)

func wikiRequest(lang testutil.Lang, args map[string]string, output any) error {
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

func getTextFromWikipedia(lang testutil.Lang, article string) (string, error) {
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

func getAllLanguages(article string) (map[testutil.Lang]string, error) {
	allLanguages := make(map[testutil.Lang]string)
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
						Lang testutil.Lang `json:"lang"`
						Path string        `json:"*"`
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
				if langlink.Lang.Known() {
					allLanguages[langlink.Lang] = langlink.Path
				}
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

func colldump(collation string, input []byte) []byte {
	cmd := exec.Command("colldump", "--test", collation)
	cmd.Stdin = bytes.NewReader(input)
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

func main() {
	var defaults = collations.Local()
	var collationsForLanguage = make(map[testutil.Lang][]collations.Collation)
	var allcollations = defaults.AllCollations()
	for lang := range testutil.KnownLanguages {
		for _, coll := range allcollations {
			if lang.MatchesCollation(coll.Name()) {
				collationsForLanguage[lang] = append(collationsForLanguage[lang], coll)
			}
		}
	}

	var rootCollations = []collations.Collation{
		defaults.LookupByName("utf8mb4_0900_as_cs"),
		defaults.LookupByName("utf8mb4_0900_as_ci"),
		defaults.LookupByName("utf8mb4_0900_ai_ci"),
		defaults.LookupByName("utf8mb4_general_ci"),
		defaults.LookupByName("utf8mb4_bin"),
		defaults.LookupByName("utf8mb4_unicode_ci"),
		defaults.LookupByName("utf8mb4_unicode_520_ci"),
	}

	articles, err := getAllLanguages(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	var tdata = &testutil.GoldenTest{Name: os.Args[1]}

	for lang, article := range articles {
		start := time.Now()
		log.Printf("[%s] %q", lang, article)
		snippet, err := getTextFromWikipedia(lang, article)
		if err != nil {
			log.Printf("error: %v", err)
			continue
		}
		log.Printf("[%s] %v", lang, time.Since(start))

		gcase := testutil.GoldenCase{
			Lang:    lang,
			Text:    []byte(snippet),
			Weights: make(map[string][]byte),
		}

		var total int
		var collationNames []string
		var interestingCollations []collations.Collation
		interestingCollations = append(interestingCollations, rootCollations...)
		interestingCollations = append(interestingCollations, collationsForLanguage[lang]...)

		for _, collation := range interestingCollations {
			transcoded, err := charset.ConvertFromUTF8(nil, collation.Charset(), []byte(snippet))
			if err != nil {
				log.Printf("[%s] skip collation %s", lang, collation.Name())
				continue
			}

			weights := colldump(collation.Name(), transcoded)
			gcase.Weights[collation.Name()] = weights
			total += len(weights)
			collationNames = append(collationNames, collation.Name())
		}

		log.Printf("[%s] written samples for %d collations (%.02fkb): %s",
			lang, len(gcase.Weights), float64(total)/1024.0, strings.Join(collationNames, ", "))

		tdata.Cases = append(tdata.Cases, gcase)
	}

	if err := tdata.EncodeToFile(fmt.Sprintf("testdata/wiki_%x.gob.gz", os.Args[1])); err != nil {
		log.Fatal(err)
	}
}
