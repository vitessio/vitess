# Copyright 2019 The Vitess Authors.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from vtdb import base_cursor


class Cursor(base_cursor.BaseListCursor):

  def __init__(self, connection, tablet_type, single_db=False, twopc=False):
    super(Cursor, self).__init__(single_db=single_db, twopc=twopc)
    self._conn = connection
    self.tablet_type = tablet_type

  def execute(self, sql, bind_variables):
    self._clear_list_state()
    if self._handle_transaction_sql(sql):
      return
    self.results, self.rowcount, self.lastrowid, self.description = (
        self.connection._execute(sql, bind_variables, self.tablet_type))
    return self.rowcount


class StreamCursor(base_cursor.BaseStreamCursor):

  def __init__(self, connection, tablet_type):
    super(StreamCursor, self).__init__()
    self._conn = connection
    self.tablet_type = tablet_type

  def execute(self, sql, bind_variables, **kargs):
    self._clear_stream_state()
    self.generator, self.description = self.connection._stream_execute(
        sql, bind_variables, self.tablet_type)
    return 0
