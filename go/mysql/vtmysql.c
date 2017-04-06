// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#include "vtmysql.h"

// All functions must call mysql_thread_init before calling mysql. This is
// because the go runtime controls thread creation, and we don't control
// which thread these functions will be called from.

void clear_result(VT_CONN *conn);

// this macro produces a compilation-time check for a condition
// if the condition is different than zero, this will abort
// if the condition is zero, this won't generate any code
// (this was imported from Linux kernel source tree)
#define BUILD_BUG_ON(condition) ((void)sizeof(char[1 - 2*!!(condition)]))

void vt_library_init(void) {
  // we depend on linking with the 64 bits version of the MySQL library:
  // the go code depends on mysql_fetch_lengths() returning 64 bits unsigned.
  BUILD_BUG_ON(sizeof(unsigned long) - 8);
  mysql_library_init(0, 0, 0);
}

int vt_connect(
    VT_CONN *conn,
    const char *host,
    const char *user,
    const char *passwd,
    const char *db,
    unsigned int port,
    const char *unix_socket,
    const char *csname,
    unsigned long client_flag)
{
  MYSQL *c;

  mysql_thread_init();
  conn->mysql = mysql_init(0);
  c = mysql_real_connect(conn->mysql, host, user, passwd, db, port, unix_socket, client_flag);
  if(!c) {
    return 1;
  }
  return mysql_set_character_set(conn->mysql, csname);
}

void vt_close(VT_CONN *conn) {
  if (conn->mysql) {
    mysql_thread_init();
    mysql_close(conn->mysql);
    conn->mysql = 0;
  }
}

int vt_execute(VT_CONN *conn, const char *stmt_str, unsigned long length, int stream) {
  mysql_thread_init();
  clear_result(conn);

  if(mysql_real_query(conn->mysql, stmt_str, length) != 0) {
    return 1;
  }

  if(stream) {
    conn->result = mysql_use_result(conn->mysql);
  } else {
    conn->result = mysql_store_result(conn->mysql);
    conn->affected_rows = mysql_affected_rows(conn->mysql);
  }
  if(conn->result == 0) {
    if(mysql_errno(conn->mysql) != 0) {
      return 1;
    }
    conn->insert_id = mysql_insert_id(conn->mysql);
  } else {
    conn->num_fields = mysql_num_fields(conn->result);
    conn->fields =  mysql_fetch_fields(conn->result);
  }
  return 0;
}

VT_ROW vt_fetch_next(VT_CONN *conn) {
  VT_ROW row = {0, 0, 0};
  if(conn->num_fields == 0) {
    return row;
  }

  mysql_thread_init();
  row.mysql_row = mysql_fetch_row(conn->result);
  if(!row.mysql_row) {
    if(mysql_errno(conn->mysql)) {
      row.has_error = 1;
      return row;
    }
  } else {
    row.lengths = mysql_fetch_lengths(conn->result);
  }
  return row;
}

void vt_close_result(VT_CONN *conn) {
  MYSQL_RES *result;

  if(conn->result) {
    mysql_thread_init();
    mysql_free_result(conn->result);
    clear_result(conn);
  }
  // Ignore subsequent results if any. We only
  // return the first set of results for now.
  while(mysql_next_result(conn->mysql) == 0) {
    result = mysql_store_result(conn->mysql);
    if (result) {
      while(mysql_fetch_row(result)) {
      }
      mysql_free_result(result);
    }
  }
}

void clear_result(VT_CONN *conn) {
  conn->affected_rows = 0;
  conn->insert_id = 0;
  conn->num_fields = 0;
  conn->fields = 0;
  conn->result = 0;
}

unsigned long vt_thread_id(VT_CONN *conn) {
  mysql_thread_init();
  return mysql_thread_id(conn->mysql);
}

unsigned int vt_errno(VT_CONN *conn) {
  mysql_thread_init();
  return mysql_errno(conn->mysql);
}

const char *vt_error(VT_CONN *conn) {
  mysql_thread_init();
  return mysql_error(conn->mysql);
}

const char *vt_sqlstate(VT_CONN *conn) {
  mysql_thread_init();
  return mysql_sqlstate(conn->mysql);
}
