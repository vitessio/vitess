// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#include <mysql.h>

// This API provides convenient C wrapper functions for mysql client.

// vt_library_init: Call this before everything else.
extern void vt_library_init(void);

typedef struct vt_conn {
  MYSQL        *mysql;
  my_ulonglong affected_rows;
  my_ulonglong insert_id;
  unsigned int num_fields;
  MYSQL_FIELD  *fields;
  MYSQL_RES    *result;
} VT_CONN;

// vt_connect: Create a connection. You must call vt_close even if vt_connect fails.
int vt_connect(
    VT_CONN *conn,
    const char *host,
    const char *user,
    const char *passwd,
    const char *db,
    unsigned int port,
    const char *unix_socket,
    const char *csname,
    unsigned long client_flag);
void vt_close(VT_CONN *conn);

// vt_execute: stream!=0 uses streaming (use_result). Otherwise it prefetches (store_result).
extern int vt_execute(VT_CONN *conn, const char *stmt_str, unsigned long length, int stream);

typedef struct vt_row {
  int           has_error;
  MYSQL_ROW     mysql_row;
  unsigned long *lengths;
} VT_ROW;

// vt_fetch_next: Iterate on this function until mysql_row==NULL or has_error!=0.
extern VT_ROW vt_fetch_next(VT_CONN *conn);

// vt_close_result: If vt_execute has results, you must call this before the next invocation.
extern void vt_close_result(VT_CONN *conn);

// Pass-through to mysql
extern unsigned long vt_thread_id(VT_CONN *conn);
extern unsigned int vt_errno(VT_CONN *conn);
extern const char *vt_error(VT_CONN *conn);
extern const char *vt_sqlstate(VT_CONN *conn);
