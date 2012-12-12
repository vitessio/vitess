#! /usr/bin/python

from itertools import izip
import json
import logging
import optparse

from net import gorpc
from net import bsonrpc
from vtdb import dbexceptions

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
  def __init__(self, addr, timeout, user=None, password=None):
    self.addr = addr
    self.timeout = timeout

    if bool(user) != bool(password):
      raise ValueError("You must provide either both or none of user and password.")
    self.user = user
    self.password = password
    self.use_auth = bool(user)

    self.client = bsonrpc.BsonRpcClient(self.uri, self.timeout)

  def dial(self):
    if self.client:
      self.client.close()
    if self.use_auth:
      self.authenticate()

  def close(self):
    self.client.close()

  def authenticate(self):
    challenge = self.client.call('AuthenticatorCRAMMD5.GetNewChallenge', "").reply['Challenge']
    # CRAM-MD5 authentication.
    proof = self.user + " " + hmac.HMAC(self.password, challenge).hexdigest()
    self.client.call('AuthenticatorCRAMMD5.Authenticate', {"Proof": proof})

  @property
  def uri(self):
    if self.use_auth:
      return 'http://%s/_bson_rpc_/auth' % self.addr
    return 'http://%s/_bson_rpc_' % self.addr

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
