#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""This module allows you to bring up and tear down keyspaces."""

import cgi
import json
import subprocess
import threading
import time

from vtdb import keyrange
from vtdb import vtgate_client

# implementations
from vtdb import grpc_vtgate_client  # pylint: disable=unused-import


def exec_query(conn, title, query, response, keyspace=None, kr=None):  # pylint: disable=missing-docstring
  if kr:
    # v2 cursor to address individual shards directly, for debug display
    cursor = conn.cursor(
        tablet_type="master", keyspace=keyspace,
        keyranges=[keyrange.KeyRange(kr)])
  else:
    # v3 cursor is automated
    cursor = conn.cursor(
        tablet_type="master", keyspace=keyspace, writable=True)

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
    cursor.close()
  except Exception as e:  # pylint: disable=broad-except
    response[title] = {
        "title": title,
        "error": str(e),
        }


def capture_log(port, queries):  # pylint: disable=missing-docstring
  p = subprocess.Popen(
      ["curl", "-s", "-N", "http://localhost:%d/debug/querylog" % port],
      stdout=subprocess.PIPE)
  def collect():
    for line in iter(p.stdout.readline, ""):
      query = line.split("\t")[12].strip('"')
      if not query:
        continue
      queries.append(query)
  t = threading.Thread(target=collect)
  t.daemon = True
  t.start()
  return p


def main():
  print "Content-Type: application/json\n"
  try:
    conn = vtgate_client.connect("grpc", "localhost:12346", 10.0)

    args = cgi.FieldStorage()
    query = args.getvalue("query")
    response = {}

    try:
      queries = []
      stats = capture_log(12345, queries)
      time.sleep(0.25)
      exec_query(conn, "result", query, response)
    finally:
      stats.terminate()
      time.sleep(0.25)
      response["queries"] = queries

    # user table
    exec_query(
        conn, "user0",
        "select * from user", response, keyspace="user", kr="-80")
    exec_query(
        conn, "user1",
        "select * from user", response, keyspace="user", kr="80-")

    # user_extra table
    exec_query(
        conn, "user_extra0",
        "select * from user_extra", response, keyspace="user", kr="-80")
    exec_query(
        conn, "user_extra1",
        "select * from user_extra", response, keyspace="user", kr="80-")

    # music table
    exec_query(
        conn, "music0",
        "select * from music", response, keyspace="user", kr="-80")
    exec_query(
        conn, "music1",
        "select * from music", response, keyspace="user", kr="80-")

    # music_extra table
    exec_query(
        conn, "music_extra0",
        "select * from music_extra", response, keyspace="user", kr="-80")
    exec_query(
        conn, "music_extra1",
        "select * from music_extra", response, keyspace="user", kr="80-")

    # lookup tables
    exec_query(
        conn, "user_idx", "select * from user_idx", response,
        keyspace="lookup", kr="-")
    exec_query(
        conn, "name_user_idx", "select * from name_user_idx", response,
        keyspace="lookup", kr="-")
    exec_query(
        conn, "music_user_idx",
        "select * from music_user_idx", response,
        keyspace="lookup", kr="-")

    print json.dumps(response)
  except Exception as e:  # pylint: disable=broad-except
    print json.dumps({"error": str(e)})


if __name__ == "__main__":
  main()
