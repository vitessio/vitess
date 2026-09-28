/*
Copyright 2026 The Vitess Authors.

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

package config

import _ "embed"

//go:embed init_db.sql
var DefaultInitDB string

//go:embed mycnf/default.cnf
var MycnfDefault string

//go:embed mycnf/mariadb10.cnf
var MycnfMariaDB10 string

//go:embed mycnf/mysql57.cnf
var MycnfMySQL57 string

//go:embed mycnf/mysql80.cnf
var MycnfMySQL80 string

//go:embed mycnf/mysql8026.cnf
var MycnfMySQL8026 string

//go:embed mycnf/mysql84.cnf
var MycnfMySQL84 string

//go:embed mycnf/mysql90.cnf
var MycnfMySQL90 string

//go:embed mycnf/clone.cnf
var MycnfClone string

//go:embed init_clone.sql
var InitClone string
