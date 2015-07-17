#! /usr/bin/python

from itertools import izip
import logging

from net import gorpc
from net import bsonrpc
from vtdb import dbexceptions
from vtdb import field_types

class Coord(object):
  Position = None
  ServerId = None

  def __init__(self, replPos, server_id = None):
    self.Position = replPos
    self.ServerId = server_id


def _make_row(row, conversions):
  converted_row = []
  for conversion_func, field_data in izip(conversions, row):
    if field_data is None:
      v = None
    elif conversion_func:
      v = conversion_func(field_data)
    else:
      v = field_data
    converted_row.append(v)
  return converted_row


class EventData(object):
  Category = None
  TableName = None
  PrimaryKeyFields = None
  PrimaryKeyValues = None
  Sql = None
  Timestamp = None
  GTIDField = None

  def __init__(self, raw_response):
    for key, val in raw_response.iteritems():
      self.__dict__[key] = val
    self.PkRows = []
    del self.__dict__['PrimaryKeyFields']
    del self.__dict__['PrimaryKeyValues']

    # build the conversions
    if not raw_response['PrimaryKeyFields']:
      return
    self.Fields = []
    conversions = []
    for field in raw_response['PrimaryKeyFields']:
      self.Fields.append(field['Name'])
      conversions.append(field_types.conversions.get(field['Type']))

    # and parse the results
    for pkList in raw_response['PrimaryKeyValues']:
      if not pkList:
        continue
      pk_row = tuple(_make_row(pkList, conversions))
      self.PkRows.append(pk_row)


class UpdateStreamConnection(object):
  def __init__(self, addr, timeout, user=None, password=None, encrypted=False, keyfile=None, certfile=None):
    self.client = bsonrpc.BsonRpcClient(addr, timeout, user, password, encrypted, keyfile, certfile)

  def dial(self):
    self.client.dial()

  def close(self):
    self.client.close()

  def stream_start(self, replPos):
    try:
      self.client.stream_call('UpdateStream.ServeUpdateStream', {"Position": replPos})
      response = self.client.stream_next()
      if response is None:
        return None
      return EventData(response.reply).__dict__
    except gorpc.GoRpcError as e:
      raise dbexceptions.OperationalError(*e.args)
    except:
      logging.exception('gorpc low-level error')
      raise

  def stream_next(self):
    try:
      response = self.client.stream_next()
      if response is None:
        return None
      return EventData(response.reply).__dict__
    except gorpc.AppError as e:
      raise dbexceptions.DatabaseError(*e.args)
    except gorpc.GoRpcError as e:
      raise dbexceptions.OperationalError(*e.args)
    except:
      logging.exception('gorpc low-level error')
      raise
