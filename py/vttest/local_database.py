"""Create a local Vitess database for testing."""

import glob
import logging
import os
import random
import re

from vttest import environment
from vttest import vt_processes


class LocalDatabase(object):
  """Set up a local Vitess database."""

  def __init__(self,
               topology,
               schema_dir,
               mysql_only,
               init_data_options,
               web_dir=None,
               default_schema_dir=None,
               extra_my_cnf=None,
               web_dir2=None,
               snapshot_file=None,
               charset='utf8'):
    """Initializes an object of this class.

    Args:
      topology: a vttest.VTTestTopology object describing the topology.
      schema_dir: see the documentation for the corresponding command line
          flag in run_local_database.py
      mysql_only: see the documentation for the corresponding command line
          flag in run_local_database.py
      init_data_options: an object of type InitDataOptions containing
          options configuring populating the database with initial random data.
          If the value is 'None' then the database will not be initialized
          with random data.
      web_dir: see the documentation for the corresponding command line
          flag in run_local_database.py
      default_schema_dir: a directory to use if no keyspace is found in the
          schema_dir directory.
      extra_my_cnf: additional cnf file to use for the EXTRA_MY_CNF var.
      web_dir2: see the documentation for the corresponding command line
          flag in run_local_database.py
      snapshot_file: A MySQL DB snapshot file.
      charset: MySQL charset.
    """

    self.topology = topology
    self.schema_dir = schema_dir
    self.mysql_only = mysql_only
    self.init_data_options = init_data_options
    self.web_dir = web_dir
    self.default_schema_dir = default_schema_dir
    self.extra_my_cnf = extra_my_cnf
    self.web_dir2 = web_dir2
    self.snapshot_file = snapshot_file
    self.charset = charset

  def setup(self):
    """Create a MySQL instance and all Vitess processes."""
    mysql_port = environment.get_port('mysql')
    self.directory = environment.get_test_directory()
    self.mysql_db = environment.mysql_db_class(
        self.directory, mysql_port, self.extra_my_cnf, self.snapshot_file)

    self.mysql_db.setup()
    if not self.snapshot_file:
      self.create_databases()
      self.load_schema()
    if self.init_data_options is not None:
      self.rng = random.Random(self.init_data_options.rng_seed)
      self.populate_with_random_data()
    if self.mysql_only:
      return

    vt_processes.start_vt_processes(self.directory, self.topology,
                                    self.mysql_db, self.schema_dir,
                                    charset=self.charset, web_dir=self.web_dir,
                                    web_dir2=self.web_dir2)

  def teardown(self):
    """Kill all Vitess processes and wait for them to end.

    MySQLTestDB's wrapper script will take care of mysqld.
    """
    if not self.mysql_only:
      self.kill()
      self.wait()
    self.mysql_db.teardown()
    environment.cleanup_test_directory(self.directory)

  def kill(self):
    """Kill all Vitess processes."""
    vt_processes.kill_vt_processes()

  def wait(self):
    """Wait for all Vitess processes to end."""
    vt_processes.wait_vt_processes()

  def vtgate_addr(self):
    """Get the host:port for vtgate."""
    if environment.get_protocol() == 'grpc':
      return vt_processes.vtcombo_process.grpc_addr()
    return vt_processes.vtcombo_process.addr()

  def config(self):
    """Returns a dict with enough information to be able to connect."""
    if self.mysql_only:
      return self.mysql_db.config()

    result = {
        'port': vt_processes.vtcombo_process.port,
        'socket': self.mysql_db.unix_socket(),
        'vtcombo_mysql_port': vt_processes.vtcombo_process.vtcombo_mysql_port,
    }

    if environment.get_protocol() == 'grpc':
      result['grpc_port'] = vt_processes.vtcombo_process.grpc_port
    return result

  def mysql_execute(self, queries, db_name=''):
    """Execute queries directly on MySQL.

    The queries will be executed in a single transaction.

    Args:
      queries: A list of strings. The SQL statements to execute.
      db_name: The database name to use.

    Returns:
      The results of the last query as a list of row tuples.
    """
    conn = self.mysql_db.connect(db_name)
    cursor = conn.cursor()

    for query in queries:
      cursor.execute(query)
    result = cursor.fetchall()

    cursor.close()
    # Commit all of the queries.
    conn.commit()
    conn.close()
    return result

  def create_databases(self):
    """Create a database for each shard."""

    cmds = []
    for kpb in self.topology.keyspaces:
      if kpb.served_from:
        # redirected keyspaces have no underlying database
        continue

      for spb in kpb.shards:
        db_name = spb.db_name_override
        if not db_name:
          db_name = 'vt_%s_%s' % (kpb.name, spb.name)
        cmds.append('create database `%s`' % db_name)
    logging.info('Creating databases')
    self.mysql_execute(cmds)

  def load_schema(self):
    """Load schema SQL from data files."""

    if not self.schema_dir:
      return

    if not os.path.isdir(self.schema_dir):
      raise Exception('schema_dir "%s" is not a directory.' % self.schema_dir)

    for kpb in self.topology.keyspaces:
      if kpb.served_from:
        # redirected keyspaces have no underlying database
        continue

      keyspace = kpb.name
      keyspace_dir = os.path.join(self.schema_dir, keyspace)
      schema_dir = keyspace_dir
      if not os.path.isdir(schema_dir):
        schema_dir = self.default_schema_dir
        if not schema_dir or not os.path.isdir(schema_dir):
          raise Exception(
              'No subdirectory found in schema dir %s for keyspace %s. '
              'No valid default_schema_dir (set to %s) was found. '
              'For keyspaces without an initial schema, create the '
              'directory %s and leave a README file to explain why the '
              'directory exists. '
              'Alternatively, disable loading schemas by setting --schema_dir '
              'to "" or set --default_schema_dir to a valid schema.' %
              (self.schema_dir, keyspace, self.default_schema_dir,
               keyspace_dir))

      for filepath in glob.glob(os.path.join(schema_dir, '*.sql')):
        logging.info('Loading schema for keyspace %s from file %s',
                     keyspace, filepath)
        cmds = self.get_sql_commands_from_file(filepath, schema_dir)

        # Run the cmds on each shard and cell in the keyspace.
        for spb in kpb.shards:
          db_name = spb.db_name_override
          if not db_name:
            db_name = 'vt_%s_%s' % (kpb.name, spb.name)
          self.mysql_execute(cmds, db_name=db_name)

  def populate_with_random_data(self):
    """Populates all shards with randomly generated data."""

    for kpb in self.topology.keyspaces:
      if kpb.served_from:
        # redirected keyspaces have no underlying database
        continue

      for spb in kpb.shards:
        db_name = spb.db_name_override
        if not db_name:
          db_name = 'vt_%s_%s' % (kpb.name, spb.name)
        self.populate_shard_with_random_data(db_name)

  def populate_shard_with_random_data(self, db_name):
    """Populates the given database with randomly generated data.

    Every table in the database is populated.

    Args:
      db_name: The shard database name (string).
    """

    tables = self.mysql_execute(['SHOW TABLES'], db_name)
    for table in tables:
      self.populate_table_with_random_data(db_name, table[0])

  # The number of rows inserted in a single INSERT statement.
  batch_insert_size = 1000

  def populate_table_with_random_data(self, db_name, table_name):
    """Populates the given table with randomly generated data.

    Queries the database for the table schema and then populates
    the columns with randomly generated data.

    Args:
      db_name: The shard database name (string).
      table_name: The name of the table to populate (string).
    """

    field_infos = self.mysql_execute(['DESCRIBE %s' % table_name], db_name)
    num_rows = self.rng.randint(self.init_data_options.min_table_shard_size,
                                self.init_data_options.max_table_shard_size)
    rows = []
    for _ in xrange(num_rows):
      row = []
      for field_info in field_infos:
        field_type = field_info[1]
        field_allow_nulls = (field_info[2] == 'YES')
        row.append(
            self.generate_random_field(
                table_name, field_type, field_allow_nulls))
      rows.append(row)

    # Insert 'rows' into the database in batches of size
    # self.batch_insert_size
    field_names = [field_info[0] for field_info in field_infos]
    for index in xrange(0, len(rows), self.batch_insert_size):
      self.batch_insert(db_name,
                        table_name,
                        field_names,
                        rows[index:index + self.batch_insert_size])

  def batch_insert(self, db_name, table_name, field_names, rows):
    """Inserts the rows in 'rows' into 'table_name' of database 'db_name'.

    Args:
      db_name: The name of the database containing the table.
      table_name: The name of the table to populate.
      field_names: The list of the field names in the table.
      rows: A list of tuples with each tuple containing
          the string representations of the fields.
          The order of the representation must match the order of the field
          names listed in 'field_names'.
    """

    field_names_string = ','.join(field_names)
    values_string = ','.join(['(' + ','.join(row) +')' for row in rows])
    # We use "INSERT IGNORE" to ignore duplicate key errors.
    insert_query = ('INSERT IGNORE INTO %s (%s) VALUES %s' %
                    (table_name, field_names_string, values_string))
    logging.info('Executing in database %s: %s', db_name, insert_query)
    self.mysql_execute([insert_query], db_name)

  def generate_random_field(self, table_name, field_type, field_allows_nulls):
    """Generates a random field string representation.

    By 'string representation' we mean a string that is suitable to be a part
    of an 'INSERT INTO' SQL statement.

    Args:
      table_name: The name of the table that will contain the generated field
          value. Only used for a descriptive exception message in case of
          an error.
      field_type: The field_type as given by a "DESCRIBE <table>" SQL statement.
      field_allows_nulls: Should be 'true' if this field allows NULLS.

    Returns:
      The random field.

    Raises:
      Exception: If 'field_type' is not supported.
    """

    value = None
    if field_type.startswith('tinyint'):
      value = self.random_integer(field_type, 1)
    elif field_type.startswith('smallint'):
      value = self.random_integer(field_type, 2)
    elif field_type.startswith('mediumint'):
      value = self.random_integer(field_type, 3)
    elif field_type.startswith('int'):
      value = self.random_integer(field_type, 4)
    elif field_type.startswith('bigint'):
      value = self.random_integer(field_type, 8)
    elif field_type.startswith('decimal'):
      value = self.random_decimal(field_type)
    else:
      raise Exception('Populating random data in field type: %s is not yet '
                      'supported. (table: %s)' % (field_type, table_name))
    if (field_allows_nulls and
        self.true_with_probability(self.init_data_options.null_probability)):
      return 'NULL'
    return value

  def true_with_probability(self, true_probability):
    """Returns a pseudo-random boolean.

    Args:
      true_probability: The probability to use for returning 'true'.
    Returns:
      The value 'true' is with probability 'true_probability'.
    """

    return self.rng.uniform(0, 1) < true_probability

  def random_integer(self, field_type, num_bytes):
    num_bits = 8*num_bytes
    if field_type.endswith('unsigned'):
      return '%d' % (self.rng.randint(0, 2**num_bits-1))
    return '%d' % (self.rng.randint(-2**(num_bits-1), 2**(num_bits-1)-1))

  decimal_regexp = re.compile(r'decimal\((\d+),(\d+)\)')

  def random_decimal(self, field_type):
    match = self.decimal_regexp.match(field_type)
    if match is None:
      raise Exception("Can't parse 'decimal' field type: %s" % field_type)
    num_digits_right = int(match.group(2))
    num_digits_left = int(match.group(1))-num_digits_right
    boundary = 10**num_digits_left-1
    rand = self.rng.uniform(-boundary, boundary)
    return '%.*f' % (num_digits_right, rand)

  def get_sql_commands_from_file(self, filename, source_root=None):
    """Given a file, extract an array of commands from the file.

    Automatically strips out three types of MySQL comment syntax:
    '--' at beginning of line: line removed
    '-- ': remove everything from here to line's end (note space after dashes)
    '#': remove everything from here to line's end
    MySQL's handling of C-style /* ... */ comments is weird, so we
    leave them alone for now.  See the MySQL manual 6.1.6 "Comment Syntax"
    for all the weird complications.

    Args:
      filename: the SQL source file to use.
      source_root: if specified, 'source FILENAME' lines in the SQL file will
        source the specified filename relative to source_root.

    Returns:
      A list of SQL commands.
    """
    fd = open(filename)
    lines = fd.readlines()

    inside_single_quotes = 0
    inside_double_quotes = 0
    commands = []
    cmd = ''
    for line in lines:
      # Strip newline and other trailing whitespace
      line = line.rstrip()

      if (not inside_single_quotes and not inside_double_quotes and
          line.startswith('--')):
        # Line starts with '--', skip line
        continue

      i = 0
      next_i = 0
      # Iterate through line, looking for special delimiters
      while 1:
        i = next_i
        if i >= len(line):
          break

        # By default, move to next character after this one
        next_i = i + 1

        if line[i] == '\\':
          # Next character is literal, skip this and the next character
          next_i = i + 2

        elif line[i] == "'":
          if not inside_double_quotes:
            inside_single_quotes = not inside_single_quotes

        elif line[i] == '"':
          if not inside_single_quotes:
            inside_double_quotes = not inside_double_quotes

        elif not inside_single_quotes and not inside_double_quotes:
          if line[i] == '#' or line[i:i+3] == '-- ':
            # Found unquoted "#" or "-- ", ignore rest of line
            line = line[:i]
            break

          if line[i] == ';':
            # Unquoted semicolon marks end of command
            cmd += line[:i]
            commands.append(cmd)
            cmd = ''

            # Chop off everything before and including the semicolon
            line = line[i+1:]

            # Start over at beginning of line
            next_i = 0

      # Reached end of line
      if line and not line.isspace():
        if source_root and not cmd and line.startswith('source '):
          commands.extend(self.get_sql_commands_from_file(
              os.path.join(source_root, line[7:]),
              source_root=source_root))
        else:
          cmd += line
          cmd += '\n'

    # Accept last command even if it doesn't end in semicolon
    cmd = cmd.strip()
    if cmd:
      commands.append(cmd)

    return commands

  def __enter__(self):
    self.setup()
    return self

  def __exit__(self, exc_type, exc_info, tb):
    self.teardown()
