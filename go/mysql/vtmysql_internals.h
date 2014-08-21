// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This file declares functions that are defined in libmysqlclient for internal
// use by the library itself, but are not part of the public API. Some of these
// are declared in sql_common.h, but that introduces a lot of other header
// dependencies, and in the case of MariaDB, not all of those other headers are
// provided in the libmysqlclient headers package (namely hash.h).

// NULL is defined in some flavors and not in others.
#ifndef NULL
#define NULL ((void*)0)
#endif

// vio_close is not declared anywhere in the libmysqlclient headers.
int vio_close(Vio*);

// cli_safe_read is declared in sql_common.h.
unsigned long cli_safe_read(MYSQL *mysql);

// st_mysql_methods and simple_command are declared in mysql.h in
// Google MySQL 5.1 (VERSION_ID=501xx), but were moved to sql_common.h in
// MariaDB (VERSION_ID=1000xx) and MySQL 5.6 (VERSION_ID=506xx).
#if MYSQL_VERSION_ID >= 50600 // MySQL version >= 5.6

typedef struct st_mysql_methods
{
  my_bool (*read_query_result)(MYSQL *mysql);
  my_bool (*advanced_command)(MYSQL *mysql,
                  enum enum_server_command command,
                  const unsigned char *header,
                  unsigned long header_length,
                  const unsigned char *arg,
                  unsigned long arg_length,
                  my_bool skip_check,
                              MYSQL_STMT *stmt);
  MYSQL_DATA *(*read_rows)(MYSQL *mysql,MYSQL_FIELD *mysql_fields,
               unsigned int fields);
  MYSQL_RES * (*use_result)(MYSQL *mysql);
  void (*fetch_lengths)(unsigned long *to,
            MYSQL_ROW column, unsigned int field_count);
  void (*flush_use_result)(MYSQL *mysql, my_bool flush_all_results);
  int (*read_change_user_result)(MYSQL *mysql);
#if !defined(MYSQL_SERVER) || defined(EMBEDDED_LIBRARY)
  MYSQL_FIELD * (*list_fields)(MYSQL *mysql);
  my_bool (*read_prepare_result)(MYSQL *mysql, MYSQL_STMT *stmt);
  int (*stmt_execute)(MYSQL_STMT *stmt);
  int (*read_binary_rows)(MYSQL_STMT *stmt);
  int (*unbuffered_fetch)(MYSQL *mysql, char **row);
  void (*free_embedded_thd)(MYSQL *mysql);
  const char *(*read_statistics)(MYSQL *mysql);
  my_bool (*next_result)(MYSQL *mysql);
  int (*read_rows_from_cursor)(MYSQL_STMT *stmt);
#endif
} MYSQL_METHODS;

#define simple_command(mysql, command, arg, length, skip_check) \
  (*(mysql)->methods->advanced_command)(mysql, command, 0,  \
                                        0, arg, length, skip_check, NULL)

#endif // MYSQL_VERSION_ID >= 50600
