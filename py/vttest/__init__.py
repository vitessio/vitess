# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from vttest import environment
from vttest import mysql_db_mysqlctl

environment.mysql_db_class = mysql_db_mysqlctl.MySqlDBMysqlctl
