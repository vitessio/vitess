#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""This module allows you to bring up and tear down keyspaces."""

import cgi
import sys
import json
import subprocess
import threading
import time
import datetime

from vtdb import vtgatev2


def exec_query_insert(table, cursor, query):
  error_insert=1
  base = 1000000
  time = datetime.datetime.now()
  for i in range(1, 200000):
    try:
      if i%2 == 0:
        modeoftransport = 'Surface'
      else:
        modeoftransport = 'Flight'
      shipmentid = 'FMCP' + str(base+i)
      bagid = str(base + i)
      currentDateTime = str(datetime.datetime.now())
      if table == 'shipment':
        query = 'insert into shipment(trackingid,createdatetime) values (\'' + shipmentid + '\',\'' + currentDateTime + '\')'      
      elif table == 'bag':
        query = 'insert into bag(isactive,createdatetime) values (' + str(i%2) + ',\'' + currentDateTime + '\')'
      elif table == 'consignment':
        query = 'insert into consignment(modeoftransport,createdatetime) values (\'' + modeoftransport + '\',\'' + currentDateTime + '\')'
      cursor.begin()
      cursor.execute(query,{})
      cursor.commit()
      if error_insert == 0:
         error_insert=1
         print '[Insert] Success : -' + str(datetime.datetime.now().isoformat())
         diff = datetime.datetime.now() - time
         print '[Insert] DownTime : - '+str(diff)
    except Exception as e:
      print str(e)
      time = datetime.datetime.now()
      print 'failure - '+str(datetime.datetime.now().isoformat())
      error_insert=0
  cursor.close()

def exec_query_select(table, cursor, query):
  error_insert=1
  base = 1000000
  time = datetime.datetime.now()
  for i in range(1, 200000):
    try:
      currentDateTime = str(datetime.datetime.now())
      if table == 'shipment':
        query = 'select * from shipment where shipmentid =' + str(i)   
      elif table == 'bag':
        query = 'select * from bag where bagid =' + str(i)
      elif table == 'consignment':
        query = 'select * from consignment where consignmentid =' + str(i)
      cursor.execute(query,{})
      if error_insert == 0:
         error_insert=1
         print '[Select] Success : -' + str(datetime.datetime.now().isoformat())
         diff = datetime.datetime.now() - time
         print '[Select] DownTime : - '+str(diff)
    except Exception as e:
      print str(e)
      time = datetime.datetime.now()
      print 'failure - '+str(datetime.datetime.now().isoformat())
      error_insert=0
  cursor.close()

def exec_query_count(table, cursor, query):
  error_insert=1
  base = 1000000
  time = datetime.datetime.now()
  for i in range(1, 2):
    try:
      currentDateTime = str(datetime.datetime.now())
      if table == 'shipment':
        query = 'select count(*) from shipment'
      elif table == 'bag':
        query = 'select count(*) from bag'
      elif table == 'consignment':
        query = 'select count(*) from consignment'
      cursor.execute(query,{})
      print cursor.fetchall()
      if error_insert == 0:
         error_insert=1
         print '[Select] Success : -' + str(datetime.datetime.now().isoformat())
         diff = datetime.datetime.now() - time
         print '[Select] DownTime : - '+str(diff)
    except Exception as e:
      print str(e)
      time = datetime.datetime.now()
      print 'failure - '+str(datetime.datetime.now().isoformat())
      error_insert=0
  cursor.close()


def exec_query_update(table, cursor, query):
  error_insert=1
  base = 1000000
  time = datetime.datetime.now()
  for i in range(1, 200000):
    try:
      currentDateTime = str(datetime.datetime.now())
      if table == 'shipment':
        query = 'update shipment set createdatetime=\''+currentDateTime+ '\' where shipmentid =' + str(i)    
      elif table == 'bag':
        query = 'update bag set createdatetime=\''+ currentDateTime + '\' where bagid =' + str(i)
      elif table == 'consignment':
        query = 'update consignment set createdatetime=\''+ currentDateTime + '\' where consignmentid =' + str(i)
      cursor.begin()
      cursor.execute(query,{})
      cursor.commit()
      if error_insert == 0:
         error_insert=1
         print '[Update] Success : -' + str(datetime.datetime.now().isoformat())
         diff = datetime.datetime.now() - time
         print '[Update] DownTime : - '+str(diff)
    except Exception as e:
      print str(e)
      time = datetime.datetime.now()
      print 'failure - '+str(datetime.datetime.now().isoformat())
      error_insert=0
  cursor.close()

def exec_query_delete(table, cursor, query):
  error_insert=1
  time = datetime.datetime.now()
  for i in range(1, 200000):
    try:
      if table == 'shipment':
        query = 'delete from shipment where shipmentid =' + str(i)    
      elif table == 'bag':
        query = 'delete from bag where bagid =' + str(i)
      elif table == 'consignment':
        query = 'delete from consignment where consignmentid =' + str(i)
      cursor.begin()
      cursor.execute(query,{})
      cursor.commit()
      if error_insert == 0:
         error_insert=1
         print '[Delete] Success : -' + str(datetime.datetime.now().isoformat())
         diff = datetime.datetime.now() - time
         print '[Delete] DownTime : - '+str(diff)
    except Exception as e:
      print str(e)
      time = datetime.datetime.now()
      print 'failure - '+str(datetime.datetime.now().isoformat())
      error_insert=0
  cursor.close()

def main():
  print "Content-Type: application/json\n"
  total = len(sys.argv)
  tablet_type = 'master'
  if total == 5:
    tablet_type = sys.argv[4]
  if total <4:
    print("Usage: insertdata.py <server> <table> <operation>")
    sys.exit()
  server = sys.argv[1]
  table = sys.argv[2]
  operation = sys.argv[3]
  try:
    conn = vtgatev2.connect([server], 10.0)
    cursor = conn.cursor(None, tablet_type, writable=True)
    if operation == 'insert':
      exec_query_insert(table,cursor,"")
    elif operation == 'update':
      exec_query_update(table,cursor,"")
    elif operation == 'select':
      exec_query_select(table,cursor,"")
    elif operation == 'delete':
      exec_query_delete(table,cursor,"")
    elif operation == 'count':
      exec_query_count(table,cursor,"")
    
  except Exception as e:
    print json.dumps({"error": str(e)})


if __name__ == "__main__":
  main()
