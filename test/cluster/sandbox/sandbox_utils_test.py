"""Tests for sandbox_utils."""

import StringIO
import sys
import unittest

import sandbox_utils


class TestComponent(object):

  def __init__(self, name, dependencies):
    self.name = name
    self.dependencies = dependencies

  def start(self):
    print 'Start %s' % self.name

  def stop(self):
    print 'Stop %s' % self.name

  def is_up(self):
    return True

  def is_down(self):
    return True


class SandboxUtilsTest(unittest.TestCase):

  def test_fix_shard_name(self):
    self.assertEquals(sandbox_utils.fix_shard_name('-80'), 'x80')
    self.assertEquals(sandbox_utils.fix_shard_name('80-'), '80x')
    self.assertEquals(sandbox_utils.fix_shard_name('40-80'), '40-80')

  def _test_dependency_graph(self, components, expected_output,
                             subcomponents=None, start=True):
    graph = sandbox_utils.create_dependency_graph(
        components, reverse=not start, subgraph=subcomponents)
    saved_stdout = sys.stdout
    try:
      out = StringIO.StringIO()
      sys.stdout = out
      sandbox_utils.execute_dependency_graph(graph, start)
      output = out.getvalue().strip()
      self.assertEquals(output, expected_output)
    finally:
      sys.stdout = saved_stdout

  def test_dependency_graph(self):
    a = TestComponent('a', ['b'])
    b = TestComponent('b', ['c'])
    c = TestComponent('c', [])
    self._test_dependency_graph(
        [a, b, c], 'Start c\nStart b\nStart a', start=True)
    self._test_dependency_graph(
        [a, b, c], 'Start a\nStart c',
        subcomponents=['a', 'c'], start=True)
    self._test_dependency_graph(
        [a, b, c], 'Start c\nStart b',
        subcomponents=['b', 'c'], start=True)
    self._test_dependency_graph(
        [a, b, c], 'Stop a\nStop b\nStop c', start=False)
    self._test_dependency_graph(
        [a, b, c], 'Stop a\nStop c',
        subcomponents=['a', 'c'], start=False)

  def test_cyclical_dependency_graph(self):
    a = TestComponent('a', ['b'])
    b = TestComponent('b', ['c'])
    c = TestComponent('c', ['a'])
    with self.assertRaises(sandbox_utils.DependencyError):
      self._test_dependency_graph([a, b, c], '')


if __name__ == '__main__':
  unittest.main()
