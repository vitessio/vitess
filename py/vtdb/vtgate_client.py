# Copyright 2017 Google Inc.
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
"""This module defines the vtgate client interface.
"""

from vtdb import vtgate_cursor

# mapping from protocol to python class.
vtgate_client_conn_classes = dict()


def register_conn_class(protocol, c):
  """Used by implementations to register themselves.

  Args:
    protocol: short string to document the protocol.
    c: class to register.
  """
  vtgate_client_conn_classes[protocol] = c


def connect(protocol, vtgate_addrs, timeout, *pargs, **kargs):
  """connect will return a dialed VTGateClient connection to a vtgate server.

  FIXME(alainjobart): exceptions raised are not consistent.

  Args:
    protocol: the registered protocol to use.
    vtgate_addrs: single or multiple vtgate server addresses to connect to.
      Which address is actually used depends on the load balancing
      capabilities of the underlying protocol used.
    timeout: connection timeout, float in seconds.
    *pargs: passed to the registered protocol __init__ method.
    **kargs: passed to the registered protocol __init__ method.

  Returns:
    A dialed VTGateClient.

  Raises:
    dbexceptions.OperationalError: if we are unable to establish the connection
      (for instance, no available instance).
    dbexceptions.Error: if vtgate_addrs have the wrong type.
    ValueError: If the protocol is unknown, or vtgate_addrs are malformed.
  """
  if protocol not in vtgate_client_conn_classes:
    raise ValueError('Unknown vtgate_client protocol', protocol)
  conn = vtgate_client_conn_classes[protocol](
      vtgate_addrs, timeout, *pargs, **kargs)
  conn.dial()
  return conn


# Note: Eventually, this object will be replaced by a proto3 CallerID
# object when all vitess customers have migrated to proto3.
class CallerID(object):
  """An object with principal, component, and subcomponent fields."""

  def __init__(self, principal=None, component=None, subcomponent=None):
    self.principal = principal
    self.component = component
    self.subcomponent = subcomponent


class VTGateClient(object):
  """VTGateClient is the interface for the vtgate client implementations.

  All implementations must implement all these methods.
  If something goes wrong with the connection, this object will be thrown out.

  FIXME(alainjobart) transactional state (the Session object) is currently
  maintained by this object. It should be maintained by the cursor, and just
  returned / passed in with every method that makes sense.
  """

  def __init__(self, addr, timeout, *pargs, **kwargs):
    """Initialize a vtgate connection.

    Args:
      addr: server address. Can be protocol dependent.
      timeout: connection timeout (float, in seconds).
      *pargs: passed to super constructor.
      **kwargs: passed to super constructor.
    """
    super(VTGateClient, self).__init__(*pargs, **kwargs)
    self.addr = addr
    self.timeout = timeout
    # self.session is used by vtgate_utils.exponential_backoff_retry.
    # implementations should use it to store the session object.
    self.session = None

  def dial(self):
    """Dial to the server.

    If successful, call close() to close the connection.
    """
    raise NotImplementedError('Child class needs to implement this')

  def close(self):
    """Close the connection.

    This object may be re-used again by calling dial().
    """
    raise NotImplementedError('Child class needs to implement this')

  def is_closed(self):
    """Checks the connection status.

    Returns:
      True if this connection is closed.
    """
    raise NotImplementedError('Child class needs to implement this')

  def cursor(self, *pargs, **kwargs):
    """Creates a cursor instance associated with this connection.

    Args:
      *pargs: passed to the cursor constructor.
      **kwargs: passed to the cursor constructor.

    Returns:
      A new cursor to use on this connection.
    """
    cursorclass = kwargs.pop('cursorclass', None) or vtgate_cursor.VTGateCursor
    return cursorclass(self, *pargs, **kwargs)

  def begin(self, effective_caller_id=None, single_db=False):
    """Starts a transaction.

    FIXME(alainjobart): instead of storing the Session as member variable,
    should return it and let the cursor store it.

    Args:
      effective_caller_id: CallerID Object.
      single_db: True if single db transaction is needed.

    Raises:
      dbexceptions.TimeoutError: for connection timeout.
      dbexceptions.TransientError: the server is overloaded, and this query
        is asked to back off.
      dbexceptions.IntegrityError: integrity of an index would not be
        guaranteed with this statement.
      dbexceptions.DatabaseError: generic database error.
      dbexceptions.ProgrammingError: the supplied statements are invalid,
        this is probably an error in the code.
      dbexceptions.FatalError: this query should not be retried.
    """
    raise NotImplementedError('Child class needs to implement this')

  def commit(self, twopc=False):
    """Commits the current transaction.

    FIXME(alainjobart): should take the session in.

    Args:
      twopc: perform 2-phase commit.

    Raises:
      dbexceptions.TimeoutError: for connection timeout.
      dbexceptions.TransientError: the server is overloaded, and this query
        is asked to back off.
      dbexceptions.IntegrityError: integrity of an index would not be
        guaranteed with this statement.
      dbexceptions.DatabaseError: generic database error.
      dbexceptions.ProgrammingError: the supplied statements are invalid,
        this is probably an error in the code.
      dbexceptions.FatalError: this query should not be retried.
    """
    raise NotImplementedError('Child class needs to implement this')

  def rollback(self):
    """Rolls the current transaction back.

    FIXME(alainjobart): should take the session in.

    Raises:
      dbexceptions.TimeoutError: for connection timeout.
      dbexceptions.TransientError: the server is overloaded, and this query
        is asked to back off.
      dbexceptions.IntegrityError: integrity of an index would not be
        guaranteed with this statement.
      dbexceptions.DatabaseError: generic database error.
      dbexceptions.ProgrammingError: the supplied statements are invalid,
        this is probably an error in the code.
      dbexceptions.FatalError: this query should not be retried.
    """
    raise NotImplementedError('Child class needs to implement this')

  def _execute(self, sql, bind_variables, tablet_type,
               keyspace_name=None,
               shards=None,
               keyspace_ids=None,
               keyranges=None,
               entity_keyspace_id_map=None, entity_column_name=None,
               not_in_transaction=False, effective_caller_id=None,
               include_event_token=False, compare_event_token=None,
               **kwargs):
    """Executes the given sql.

    FIXME(alainjobart): should take the session in.

    Args:
      sql: query to execute.
      bind_variables: map of bind variables for the query.
      tablet_type: the (string) version of the tablet type.
      keyspace_name: if specified, the keyspace to send the query to.
        Required if any of the routing parameters is used.
        Not required only if using vtgate v3 API.
      shards: if specified, use this list of shards names to route the query.
        Incompatible with keyspace_ids, keyranges, entity_keyspace_id_map,
        entity_column_name.
        Requires keyspace.
      keyspace_ids: if specified, use this list to route the query.
        Incompatible with shards, keyranges, entity_keyspace_id_map,
        entity_column_name.
        Requires keyspace.
      keyranges: if specified, use this list to route the query.
        Incompatible with shards, keyspace_ids, entity_keyspace_id_map,
        entity_column_name.
        Requires keyspace.
      entity_keyspace_id_map: if specified, use this map to route the query.
        Incompatible with shards, keyspace_ids, keyranges.
        Requires keyspace, entity_column_name.
      entity_column_name: if specified, use this value to route the query.
        Incompatible with shards, keyspace_ids, keyranges.
        Requires keyspace, entity_keyspace_id_map.
      not_in_transaction: force this execute to be outside the current
        transaction, if any.
      effective_caller_id: CallerID object.
      include_event_token: if true, the flag will be sent to vtgate.
        The member variable event_token will be set with the result.
      compare_event_token: set the result extras fresher based on this token.
        The member variable fresher will be set with the result.
      **kwargs: implementation specific parameters.

    Returns:
      results: list of rows.
      rowcount: how many rows were affected.
      lastrowid: auto-increment value for the last row inserted.
      fields: describes the field names and types.

    Raises:
      dbexceptions.TimeoutError: for connection timeout.
      dbexceptions.TransientError: the server is overloaded, and this query
        is asked to back off.
      dbexceptions.IntegrityError: integrity of an index would not be
        guaranteed with this statement.
      dbexceptions.DatabaseError: generic database error.
      dbexceptions.ProgrammingError: the supplied statements are invalid,
        this is probably an error in the code.
      dbexceptions.FatalError: this query should not be retried.
    """
    raise NotImplementedError('Child class needs to implement this')

  def _execute_batch(
      self, sql_list, bind_variables_list, tablet_type,
      keyspace_list=None, shards_list=None, keyspace_ids_list=None,
      as_transaction=False, effective_caller_id=None, **kwargs):
    """Executes a list of sql queries.

    These follow the same routing rules as _execute.

    FIXME(alainjobart): should take the session in.

    Args:
      sql_list: list of SQL queries to execute.
      bind_variables_list: bind variables to associated with each query.
      tablet_type: the (string) version of the tablet type.
      keyspace_list: if specified, the keyspaces to send the queries to.
        Required if any of the routing parameters is used.
        Not required only if using vtgate v3 API.
      shards_list: if specified, use this list of shards names (per sql query)
        to route each query.
        Incompatible with keyspace_ids_list.
        Requires keyspace_list.
      keyspace_ids_list: if specified, use this list of keyspace_ids (per sql
        query) to route each query.
        Incompatible with shards_list.
        Requires keyspace_list.
      as_transaction: starts and commits a transaction around the statements.
      effective_caller_id: CallerID object.
      **kwargs: implementation specific parameters.

    Returns:
      results: an array of (results, rowcount, lastrowid, fields) tuples,
        one for each query.

    Raises:
      dbexceptions.TimeoutError: for connection timeout.
      dbexceptions.TransientError: the server is overloaded, and this query
        is asked to back off.
      dbexceptions.IntegrityError: integrity of an index would not be
        guaranteed with this statement.
      dbexceptions.DatabaseError: generic database error.
      dbexceptions.ProgrammingError: the supplied statements are invalid,
        this is probably an error in the code.
      dbexceptions.FatalError: this query should not be retried.
    """
    raise NotImplementedError('Child class needs to implement this')

  def _stream_execute(
      self, sql, bind_variables, tablet_type, keyspace=None, shards=None,
      keyspace_ids=None, keyranges=None, effective_caller_id=None, **kwargs):
    """Executes the given sql, in streaming mode.

    FIXME(alainjobart): the return values are weird (historical reasons)
    and unused for now. We should use them, and not store the current
    streaming status in the connection, but in the cursor.

    Args:
      sql: query to execute.
      bind_variables: map of bind variables for the query.
      tablet_type: the (string) version of the tablet type.
      keyspace: if specified, the keyspace to send the query to.
        Required if any of the routing parameters is used.
        Not required only if using vtgate v3 API.
      shards: if specified, use this list of shards names to route the query.
        Incompatible with keyspace_ids, keyranges.
        Requires keyspace.
      keyspace_ids: if specified, use this list to route the query.
        Incompatible with shards, keyranges.
        Requires keyspace.
      keyranges: if specified, use this list to route the query.
        Incompatible with shards, keyspace_ids.
        Requires keyspace.
      effective_caller_id: CallerID object.
      **kwargs: implementation specific parameters.

    Returns:
      A (row generator, fields) pair.

    Raises:
      dbexceptions.TimeoutError: for connection timeout.
      dbexceptions.TransientError: the server is overloaded, and this query
        is asked to back off.
      dbexceptions.IntegrityError: integrity of an index would not be
        guaranteed with this statement.
      dbexceptions.DatabaseError: generic database error.
      dbexceptions.ProgrammingError: the supplied statements are invalid,
        this is probably an error in the code.
      dbexceptions.FatalError: this query should not be retried.
    """
    raise NotImplementedError('Child class needs to implement this')

  def get_srv_keyspace(self, keyspace):
    """Returns a SrvKeyspace object.

    Args:
      keyspace: name of the keyspace to retrieve.

    Returns:
      srv_keyspace: a keyspace.Keyspace object.

    Raises:
      TBD
    """
    raise NotImplementedError('Child class needs to implement this')

  def update_stream(self,
                    keyspace_name, tablet_type,
                    timestamp=None, event=None,
                    shard=None, key_range=None,
                    effective_caller_id=None,
                    **kwargs):
    """Asks for an update stream.

    Args:
      keyspace_name: the keyspace to get updates from.
      tablet_type: the (proto3) version of the tablet type.
      timestamp: when to start the stream from. Unused if event is set,
        and event.shard matches the only shard we stream from.
      event: query_pb2.EventToken to start streaming from. Used only if its
        shard field matches the single shard we're going to stream from.
      shard: the shard name to listen for.
        Incompatible with key_range.
      key_range: the key range to listen for.
        Incompatible with shard.
      effective_caller_id: CallerID object.
      **kwargs: implementation specific parameters.

    Returns:
      A row generator that returns tuples (event, resume timestamp).

    Raises:
      dbexceptions.TimeoutError: for connection timeout.
      dbexceptions.TransientError: the server is overloaded, and this query
        is asked to back off.
      dbexceptions.DatabaseError: generic database error.
      dbexceptions.FatalError: this query should not be retried.
    """
    raise NotImplementedError('Child class needs to implement this')

  def message_stream(self,
                     keyspace, name,
                     shard=None, key_range=None,
                     effective_caller_id=None,
                     **kwargs):
    """Asks for a message stream.

    Args:
      keyspace: the keyspace of the message table.
      name: the name of the message table.
      shard: the shard name to listen for.
        Incompatible with key_range.
      key_range: the key range to listen for.
        Incompatible with shard.
      effective_caller_id: CallerID object.
      **kwargs: implementation specific parameters.

    Returns:
      A (row generator, fields) pair.

    Raises:
      dbexceptions.TimeoutError: for connection timeout.
      dbexceptions.TransientError: the server is overloaded, and this query
        is asked to back off.
      dbexceptions.DatabaseError: generic database error.
      dbexceptions.FatalError: this query should not be retried.
    """
    raise NotImplementedError('Child class needs to implement this')

  def message_ack(self,
                  name, ids,
                  keyspace=None, effective_caller_id=None,
                  **kwargs):
    """Acks a list of messages.

    Args:
      name: the name of the message table.
      ids: list of message ids to ack.
      keyspace: the keyspace of the message table.
        Not required if table can be auto-resolved.
      effective_caller_id: CallerID object.
      **kwargs: implementation specific parameters.

    Returns:
      The number of rows acked.

    Raises:
      dbexceptions.TimeoutError: for connection timeout.
      dbexceptions.TransientError: the server is overloaded, and this query
        is asked to back off.
      dbexceptions.DatabaseError: generic database error.
      dbexceptions.FatalError: this query should not be retried.
    """
    raise NotImplementedError('Child class needs to implement this')
