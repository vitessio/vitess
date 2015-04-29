"""Collection of utilities to validate database input.

This module contains utilities that validate database input and perform
actions for invalid input.
"""


def invalid_utf8_columns(utf8_columns, column_values):
  """Return a list of column names that have invalid utf-8 encoded values.

  Args:
    utf8_columns: A list of columns to validate for well formed utf-8.
    column_values: A dict containing column names and values, or list
                   of (key, value) tuples.

  Returns:
    A list of columns (always a sub-set of utf8_columns) that contain
    poorly formed utf-8 in column_values.
  """
  invalid_columns = []
  if isinstance(column_values, dict):
    column_values = column_values.iteritems()

  for column_name, column_value in column_values:
    if column_value and column_name in utf8_columns and not isinstance(
        column_value, unicode):
      try:
        column_value.decode('utf-8', errors='strict')
      except UnicodeError:
        invalid_columns.append(column_name)

  return invalid_columns
