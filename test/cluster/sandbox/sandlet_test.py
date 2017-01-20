"""Sandlet tests."""

import StringIO
import sys
import unittest

import sandlet


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


class SandletTest(unittest.TestCase):

  def _test_dependency_graph(
      self, components, expected_output, action, subcomponents=None):
    saved_stdout = sys.stdout
    group = sandlet.ComponentGroup()
    for c in components:
      group.add_component(c)
    try:
      out = StringIO.StringIO()
      sys.stdout = out
      group.execute(action, subcomponents)
      output = out.getvalue().strip()
      self.assertEquals(output, expected_output)
    finally:
      sys.stdout = saved_stdout

  def test_dependency_graph(self):
    a = TestComponent('a', ['b'])
    b = TestComponent('b', ['c'])
    c = TestComponent('c', [])
    self._test_dependency_graph(
        [a, b, c], 'Start c\nStart b\nStart a', sandlet.StartAction)
    self._test_dependency_graph(
        [a, b, c], 'Start a\nStart c', sandlet.StartAction,
        subcomponents=['a', 'c'])
    self._test_dependency_graph(
        [a, b, c], 'Start c\nStart b', sandlet.StartAction,
        subcomponents=['b', 'c'])
    self._test_dependency_graph(
        [a, b, c], 'Stop a\nStop b\nStop c', sandlet.StopAction)
    self._test_dependency_graph(
        [a, b, c], 'Stop a\nStop c', sandlet.StopAction,
        subcomponents=['a', 'c'])

  def test_cyclical_dependency_graph(self):
    a = TestComponent('a', ['b'])
    b = TestComponent('b', ['c'])
    c = TestComponent('c', ['a'])
    with self.assertRaises(sandlet.DependencyError):
      self._test_dependency_graph([a, b, c], '', sandlet.StartAction)


if __name__ == '__main__':
  unittest.main()
