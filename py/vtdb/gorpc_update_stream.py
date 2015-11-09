
# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from net import bsonrpc
from net import gorpc
from vtdb import dbexceptions
from vtdb import field_types_proto3
from vtdb import update_stream


def _make_row(row, conversions):
  """Builds a python native row from proto3 over bsonrpc row."""
  converted_row = []
  offset = 0
  for i, l in enumerate(row['Lengths']):
    if l == -1:
      converted_row.append(None)
    elif conversions[i]:
      converted_row.append(conversions[i](row['Values'][offset:offset+l]))
      offset += l
    else:
      converted_row.append(row['Values'][offset:offset+l])
      offset += l
  return converted_row


class GoRpcUpdateStreamConnection(update_stream.UpdateStreamConnection):
  """The go rpc implementation of UpdateStreamConnection.

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
                              {'Position': position})
      while True:
        response = self.client.stream_next()
        if response is None:
          break
        reply = response.reply

        fields = []
        rows = []
        if reply['PrimaryKeyFields']:
          conversions = []
          for field in reply['PrimaryKeyFields']:
            fields.append(field['Name'])
            conversions.append(field_types_proto3.conversions.get(
                field['Type']))

          for pk_list in reply['PrimaryKeyValues']:
            if not pk_list:
              continue
            decoded_row = tuple(_make_row(pk_list, conversions))
            rows.append(decoded_row)

        yield update_stream.StreamEvent(category=reply['Category'],
                                        table_name=reply['TableName'],
                                        fields=fields,
                                        rows=rows,
                                        sql=reply['Sql'],
                                        timestamp=reply['Timestamp'],
                                        transaction_id=reply['TransactionId'])
    except gorpc.AppError as e:
      raise dbexceptions.DatabaseError(*e.args)
    except gorpc.GoRpcError as e:
      raise dbexceptions.OperationalError(*e.args)
    except:
      raise

update_stream.register_conn_class('gorpc', GoRpcUpdateStreamConnection)
