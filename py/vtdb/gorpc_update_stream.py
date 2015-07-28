# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from itertools import izip
import logging

from net import gorpc
from net import bsonrpc
from vtdb import dbexceptions
from vtdb import field_types
from vtdb import update_stream


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


class GoRpcUpdateStreamConnection(update_stream.UpdateStreamConnection):
  """GoRpcUpdateStreamConnection is the go rpc implementation of
  UpdateStreamConnection.
  It is registered as 'gorpc' protocol.
  """

  def __init__(self, addr, timeout, user=None, password=None,
               keyfile=None, certfile=None):
    self.addr = addr
    self.timeout = timeout
    self.client = bsonrpc.BsonRpcClient(addr, timeout, user=user,
                                        password=password,
                                        keyfile=keyfile, certfile=certfile)
    self.connected = False

  def __str__(self):
    return '<GoRpcUpdateStreamConnection %s>' % self.addr

  def dial(self):
    if self.connected:
      self.client.close()

    self.client.dial()
    self.connected = True

  def close(self):
    self.connected = False
    self.client.close()

  def is_closed(self):
    return self.client.is_closed()

  def stream_update(self, position, timeout=3600.0):
    """Note this implementation doesn't honor the timeout."""
    try:
      self.client.stream_call('UpdateStream.ServeUpdateStream',
                              {"Position": position})
      while True:
        response = self.client.stream_next()
        if response is None:
          break
        reply = response.reply

        str_category = reply['Category']
        if str_category == 'DML':
          category = update_stream.StreamEvent.DML
        elif str_category == 'DDL':
          category = update_stream.StreamEvent.DDL
        elif str_category == 'POS':
          category = update_stream.StreamEvent.POS
        else:
          category = update_stream.StreamEvent.ERR

        fields = []
        rows = []
        if reply['PrimaryKeyFields']:
          conversions = []
          for field in reply['PrimaryKeyFields']:
            fields.append(field['Name'])
            conversions.append(field_types.conversions.get(field['Type']))

          for pk_list in reply['PrimaryKeyValues']:
            if not pk_list:
              continue
            row = tuple(_make_row(pk_list, conversions))
            rows.append(row)

        yield update_stream.StreamEvent(category=category,
                                        table_name=reply['TableName'],
                                        fields=fields,
                                        rows=rows,
                                        sql=reply['Sql'],
                                        timestamp=reply['Timestamp'],
                                        transaction_id=reply['TransactionID'])
    except gorpc.AppError as e:
      raise dbexceptions.DatabaseError(*e.args)
    except gorpc.GoRpcError as e:
      raise dbexceptions.OperationalError(*e.args)
    except:
      raise

update_stream.register_conn_class('gorpc', GoRpcUpdateStreamConnection)
