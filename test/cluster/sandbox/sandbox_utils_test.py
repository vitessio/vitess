"""Tests for sandbox_utils."""

import unittest

import sandbox_utils


class SandboxUtilsTest(unittest.TestCase):

  def test_fix_shard_name(self):
    self.assertEquals(sandbox_utils.fix_shard_name('-80'), 'x80')
    self.assertEquals(sandbox_utils.fix_shard_name('80-'), '80x')
    self.assertEquals(sandbox_utils.fix_shard_name('40-80'), '40-80')


if __name__ == '__main__':
  unittest.main()
