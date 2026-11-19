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

// Package filecustomrule implements static custom rule from a config file
package filecustomrule

import (
	"log/slog"
	"os"
	"path"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

var (
	// Actual FileCustomRule object in charge of rule updates
	fileCustomRule = NewFileCustomRule()

	// Commandline flag to specify rule path
	fileRulePath        string
	fileRuleShouldWatch bool
)

func registerFlags(fs *pflag.FlagSet) {
	fs.StringVar(&fileRulePath, "filecustomrules", fileRulePath, "file based custom rule path")
	utils.SetFlagBoolVar(fs, &fileRuleShouldWatch, "filecustomrules-watch", fileRuleShouldWatch, "set up a watch on the target file and reload query rules when it changes")
}

func init() {
	servenv.OnParseFor("vttablet", registerFlags)
}

// FileCustomRule is an implementation of CustomRuleManager, it reads custom query
// rules from local file for once and push it to vttablet
type FileCustomRule struct {
	path                    string       // Path to the file containing custom query rules
	currentRuleSet          *rules.Rules // Query rules built from local file
	currentRuleSetTimestamp int64        // Unix timestamp when currentRuleSet is built from local file
}

// FileCustomRuleSource is the name of the file based custom rule source
const FileCustomRuleSource string = "FILE_CUSTOM_RULE"

// NewFileCustomRule returns pointer to new FileCustomRule structure
func NewFileCustomRule() (fcr *FileCustomRule) {
	fcr = new(FileCustomRule)
	fcr.path = ""
	fcr.currentRuleSet = rules.New()
	return fcr
}

// ParseRules will construct a Rules object based on a file path. In the case
// of error it returns nil and that error. A log will be printed to capture the
// stage at which parsing failed.
func ParseRules(path string) (*rules.Rules, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Warn("Error reading file", slog.String("path", path), slog.Any("error", err))
		// Don't update any internal cache, just return error
		return nil, err
	}
	qrs := rules.New()
	err = qrs.UnmarshalJSON(data)
	if err != nil {
		log.Warn("Error unmarshaling query rules",
			slog.String("path", path),
			slog.Any("error", err),
			slog.String("error_code", vterrors.Code(err).String()))
		return nil, err
	}
	return qrs, nil
}

// Open try to build query rules from local file and push the rules to vttablet
func (fcr *FileCustomRule) Open(qsc tabletserver.Controller, rulePath string) error {
	fcr.path = rulePath
	if fcr.path == "" {
		// Don't go further if path is empty
		return nil
	}

	qrs, err := ParseRules(rulePath)
	if err != nil {
		return err
	}
	fcr.currentRuleSetTimestamp = time.Now().Unix()
	fcr.currentRuleSet = qrs.Copy()
	// Push query rules to vttablet
	qsc.SetQueryRules(FileCustomRuleSource, qrs.Copy())
	log.Info("Custom rule loaded from file", slog.String("path", fcr.path))
	return nil
}

// GetRules returns query rules built from local file
func (fcr *FileCustomRule) GetRules() (qrs *rules.Rules, version int64, err error) {
	return fcr.currentRuleSet.Copy(), fcr.currentRuleSetTimestamp, nil
}

// ActivateFileCustomRules activates this static file based custom rule mechanism
func ActivateFileCustomRules(qsc tabletserver.Controller) {
	if fileRulePath != "" {
		qsc.RegisterQueryRuleSource(FileCustomRuleSource)
		fileCustomRule.Open(qsc, fileRulePath)

		if !fileRuleShouldWatch {
			return
		}

		baseDir := path.Dir(fileRulePath)
		ruleFileName := path.Base(fileRulePath)

		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			log.Error("Unable to create new fsnotify watcher", slog.Any("error", err))
			os.Exit(1)
		}
		servenv.OnTerm(func() { watcher.Close() })

		go func(tsc tabletserver.Controller) {
			for {
				select {
				case evt, ok := <-watcher.Events:
					if !ok {
						return
					}
					if path.Base(evt.Name) != ruleFileName {
						continue
					}
					if err := fileCustomRule.Open(tsc, fileRulePath); err != nil {
						log.Info("Failed to load custom rules from file",
							slog.String("path", fileRulePath),
							slog.Any("error", err),
							slog.String("error_code", vterrors.Code(err).String()))
					} else {
						log.Info("Loaded custom rules from file", slog.String("path", fileRulePath))
					}
				case err, ok := <-watcher.Errors:
					if !ok {
						return
					}
					log.Error("Error watching file", slog.String("path", fileRulePath), slog.Any("error", err))
				}
			}
		}(qsc)

		if err = watcher.Add(baseDir); err != nil {
			log.Error("Unable to set up watcher",
				slog.String("dir", baseDir),
				slog.String("file_name", ruleFileName),
				slog.Any("error", err))
			os.Exit(1)
		}
	}
}

func init() {
	tabletserver.RegisterFunctions = append(tabletserver.RegisterFunctions, ActivateFileCustomRules)
}
