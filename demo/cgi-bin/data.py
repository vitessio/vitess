#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""
This module allows you to bring up and tear down keyspaces.
"""

import cgi
import json
import sys

from vtdb import vtgatev3

def exec_query(cursor, title, query, response):
  try:
    if not query or query == "undefined":
      return
    if query.startswith("select"):
      cursor.execute(query, {})
    else:
      cursor.begin()
      cursor.execute(query, {})
      cursor.commit()
    response[title] = {
        "title": title,
        "description": cursor.description,
        "rowcount": cursor.rowcount,
        "lastrowid": cursor.lastrowid,
        "results": cursor.results,
        }
  except Exception as e:
    response[title] = {
        "title": title,
        "error": str(e),
        }

def main():
  print "Content-Type: application/json\n"
  try:
    conn = vtgatev3.connect("localhost:15009", 10.0)
    cursor = conn.cursor("master")

    args = cgi.FieldStorage()
    query = args.getvalue("query")
    response = {}

    exec_query(cursor, "result", query, response)

    exec_query(cursor, "user0", "select * from user where keyrange('','\x80')", response)
    exec_query(cursor, "user1", "select * from user where keyrange('\x80', '')", response)
    exec_query(cursor, "user_extra0", "select * from user_extra where keyrange('','\x80')", response)
    exec_query(cursor, "user_extra1", "select * from user_extra where keyrange('\x80', '')", response)

    exec_query(cursor, "music0", "select * from music where keyrange('','\x80')", response)
    exec_query(cursor, "music1", "select * from music where keyrange('\x80', '')", response)
    exec_query(cursor, "music_extra0", "select * from music_extra where keyrange('','\x80')", response)
    exec_query(cursor, "music_extra1", "select * from music_extra where keyrange('\x80', '')", response)

    exec_query(cursor, "user_idx", "select * from user_idx", response)
    exec_query(cursor, "name_user_idx", "select * from name_user_idx", response)
    exec_query(cursor, "music_user_idx", "select * from music_user_idx", response)

    print json.dumps(response)
  except Exception as e:
    print json.dumps({"error": str(e)})


if __name__ == '__main__':
  main()
