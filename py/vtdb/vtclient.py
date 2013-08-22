from vtdb import dbapi
from vtdb import tablet

# Provide compatibility with the MySQLdb query param style and prune bind_vars
class VtOCCConnection(tablet.TabletConnection):
  def begin(self):
    result = tablet.TabletConnection.begin(self)
    return result

  def commit(self):
    result = tablet.TabletConnection.commit(self)
    return result

  def _execute(self, sql, bind_variables):
    sql, bind_variables = dbapi.prepare_query_bind_vars(sql, bind_variables)
    result = tablet.TabletConnection._execute(self, sql, bind_variables)
    return result

  def _execute_batch(self, sql_list, bind_variables_list):
    sane_sql_list = []
    sane_bind_vars_list = []
    for sql, bind_variables in zip(sql_list, bind_variables_list):
      sane_sql, sane_bind_vars = dbapi.prepare_query_bind_vars(sql, bind_variables)
      sane_sql_list.append(sane_sql)
      sane_bind_vars_list.append(sane_bind_vars)

    result = tablet.TabletConnection._execute_batch(self, sane_sql_list, sane_bind_vars_list)
    return result

  def _stream_execute(self, sql, bind_variables):
    sql, bind_variables = dbapi.prepare_query_bind_vars(sql, bind_variables)
    result = tablet.TabletConnection._stream_execute(self, sql, bind_variables)
    return result


def connect(*pargs, **kargs):
  conn = VtOCCConnection(*pargs, **kargs)
  conn.dial()
  return conn
