#!/usr/bin/env python

"""Re-runs backup.py with use_mysqlctld=True."""

import backup
import utils

if __name__ == '__main__':
  backup.use_mysqlctld = True
  utils.main(backup)
