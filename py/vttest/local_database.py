"""Create a local Vitess database for testing."""

import glob
import logging
import os

from vttest import environment
from vttest import vt_processes


class LocalDatabase(object):
  """Set up a local Vitess database."""

  def __init__(self, shards, schema_dir, vschema, mysql_only):
    self.shards = shards
    self.schema_dir = schema_dir
    self.vschema = vschema
    self.mysql_only = mysql_only

  def setup(self):
    """Create a MySQL instance and all Vitess processes."""
    mysql_port = environment.get_port('mysql')
    self.directory = environment.get_test_directory()
    self.mysql_db = environment.mysql_db_class(self.directory, mysql_port)

    self.mysql_db.setup()
    self.create_databases()
    self.load_schema()
    if self.mysql_only:
      return

    vt_processes.start_vt_processes(self.directory, self.shards, self.mysql_db, self.vschema)

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
        }
    if environment.get_protocol() == 'grpc':
      result['grpc_port'] = vt_processes.vtcombo_process.grpc_port
    return result

  def mysql_execute(self, queries, db_name=''):
    """Execute queries directly on MySQL."""
    conn = self.mysql_db.connect(db_name)
    cursor = conn.cursor()

    for query in queries:
      cursor.execute(query)
    result = cursor.fetchall()

    cursor.close()
    conn.close()
    return result

  def create_databases(self):
    """Create a database for each shard."""

    cmds = []
    for shard in self.shards:
      cmds.append('create database `%s`' % shard.db_name)
    logging.info('Creating databases')
    self.mysql_execute(cmds)

  def load_schema(self):
    """Load schema SQL from data files."""

    if not self.schema_dir:
      return

    if not os.path.isdir(self.schema_dir):
      raise Exception('schema_dir "%s" is not a directory.' % self.schema_dir)

    for keyspace in set([shard.keyspace for shard in self.shards]):
      keyspace_dir = os.path.join(self.schema_dir, keyspace)
      if not os.path.isdir(keyspace_dir):
        raise Exception(
            'No subdirectory found in schema dir %s for keyspace %s. '
            'For keyspaces without an initial schema, create the '
            'directory %s and leave a README file to explain why the '
            'directory exists. '
            'Alternatively, disable loading schemas by setting --schema_dir '
            'to "".' %
            (self.schema_dir, keyspace, keyspace_dir))

      for filepath in glob.glob(os.path.join(keyspace_dir, '*.sql')):
        logging.info('Loading schema for keyspace %s from file %s',
                     keyspace, filepath)
        cmds = self.get_sql_commands_from_file(filepath, keyspace_dir)

        # Run the cmds on each shard in the keyspace.
        for shard in self.shards:
          if shard.keyspace == keyspace:
            self.mysql_execute(cmds, db_name=shard.db_name)

  def get_sql_commands_from_file(self, filename, source_root=None):
    """Given a file, extract an array of commands from the file.

    Automatically strips out three types of MySQL comment syntax:
    '--' at beginning of line: line removed
    '-- ': remove everything from here to line's end (note space after dashes)
    '#': remove everything from here to line's end
    MySQL's handling of C-style /* ... */ comments is weird, so we
    leave them alone for now.  See the MySQL manual 6.1.6 "Comment Syntax"
    for all the weird complications.

    If source_root is specified, 'source FILENAME' lines in the SQL file will
    source the specified filename relative to source_root.
    """
    file = open(filename)
    lines = file.readlines()

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
