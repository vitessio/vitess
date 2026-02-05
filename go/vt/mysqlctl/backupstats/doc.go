/*
Copyright 2022 The Vitess Authors.

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

// Package backupstats provides a Stats interface for backup and restore
// operations in the mysqlctl package.
//
// The goal is to provide a consistent reporting interface that can be
// used by any BackupEngine, BackupStorage, or FileHandle component, so that
// those components don't have to register their own stats or reference global
// variables.
package backupstats
