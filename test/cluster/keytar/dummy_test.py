#!/usr/bin/env python
"""Dummy no-op test to be used in the webdriver test."""

import logging
import sys
import unittest


class DummyTest(unittest.TestCase):

  def test_dummy(self):
    logging.info('Dummy output.')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  del sys.argv[1:]
  unittest.main()
