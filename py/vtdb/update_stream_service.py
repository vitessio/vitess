#! /usr/bin/python

from itertools import izip
import logging

from net import gorpc
from net import bsonrpc
from vtdb import dbexceptions

class ReplPosition(object):
  MasterFilename = None
  MasterPosition = None
  RelayFilename = None
  RelayPosition = None

  def __init__(self, master_filename, master_position):
    self.MasterFilename = master_filename
    self.MasterPosition = master_position

class BinlogPosition(object):
  Position = None
  Timestamp = None
  Xid = None
  GroupId = None
  
  def __init__(self, master_filename, master_position, group_id=0): 
    self.Position = ReplPosition(master_filename, master_position).__dict__
    self.GroupId = group_id
  

class EventData(object):
  SqlType = None
  TableName = None
  Sql = None
  PkRows = None

  def __init__(self, raw_response):
    for key, val in raw_response.iteritems():
      self.__dict__[key] = val
    self.PkRows = []
    del self.__dict__['PkColNames']
    del self.__dict__['PkValues']

    if not raw_response['PkColNames']:
      return
    for pkList in raw_response['PkValues']:
      if not pkList:
        continue
      pk_row = [(col_name, col_value) for col_name, col_value in izip(raw_response['PkColNames'], pkList)]
      self.PkRows.append(pk_row)


class UpdateStreamResponse(object):
  BinlogPosition = None
  EventData = None
  Error = None

  def __init__(self, response_dict):
    self.raw_response = response_dict
    self.format()

  def format(self):
    if self.raw_response['Error'] == "":
      self.Error = None
    else:
      self.Error = self.raw_response['Error']
    self.BinlogPosition = self.raw_response['BinlogPosition']
    self.EventData = EventData(self.raw_response['EventData']).__dict__

class UpdateStreamConnection(object):
  def __init__(self, addr, timeout, user=None, password=None, encrypted=False, keyfile=None, certfile=None):
    self.client = bsonrpc.BsonRpcClient(addr, timeout, user, password, encrypted, keyfile, certfile)

  def dial(self):
    self.client.dial()

  def close(self):
    self.client.close()

  def stream_start(self, start_position):
    req = {'StartPosition':start_position}

    try:
      self.client.stream_call('UpdateStream.ServeUpdateStream', req)
      first_response = self.client.stream_next()
      update_stream_response = UpdateStreamResponse(first_response.reply)

    except gorpc.GoRpcError as e:
      raise dbexceptions.OperationalError(*e.args)
    except:
      logging.exception('gorpc low-level error')
      raise
    return update_stream_response.BinlogPosition, update_stream_response.EventData, update_stream_response.Error

  def stream_next(self):
    try:
      response = self.client.stream_next()
      if response is None:
        return None, None, None
      update_stream_response = UpdateStreamResponse(response.reply)
    except gorpc.GoRpcError as e:
      raise dbexceptions.OperationalError(*e.args)
    except:
      logging.exception('gorpc low-level error')
      raise
    return update_stream_response.BinlogPosition, update_stream_response.EventData, update_stream_response.Error
