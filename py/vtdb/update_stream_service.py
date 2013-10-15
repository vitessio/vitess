#! /usr/bin/python

from itertools import izip
import logging

from net import gorpc
from net import bsonrpc
from vtdb import dbexceptions

class ReplicationCoordinates(object):
  MasterFilename = None
  MasterPosition = None
  GroupId        = None

  def __init__(self, master_filename, master_position, group_id):
    self.MasterFilename = master_filename
    self.MasterPosition = master_position
    self.GroupId = group_id

class Coord(object):
  Position = None
  Timestamp = None
  Xid = None

  def __init__(self, master_filename, master_position, group_id=None):
    self.Position = ReplicationCoordinates(master_filename, master_position, group_id).__dict__


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
  Coord = None
  Data = None

  def __init__(self, response_dict):
    self.raw_response = response_dict
    self.format()

  def format(self):
    self.Coord = self.raw_response['Coord']
    self.Data = EventData(self.raw_response['Data']).__dict__

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
    return update_stream_response.Coord, update_stream_response.Data

  def stream_next(self):
    try:
      response = self.client.stream_next()
      if response is None:
        return None, None
      update_stream_response = UpdateStreamResponse(response.reply)
    except gorpc.AppError as e:
      raise dbexceptions.DatabaseError(*e.args)
    except gorpc.GoRpcError as e:
      raise dbexceptions.OperationalError(*e.args)
    except:
      logging.exception('gorpc low-level error')
      raise
    return update_stream_response.Coord, update_stream_response.Data
