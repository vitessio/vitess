# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Top level component of a sandbox.

A sandlet is an abstraction to be used by applications to organize a sandbox
into discrete groupings. A user can start an entire sandbox, which will start
all sandlets, or selectively choose which ones to start.

Sandlets are made up of components, which are the actual subprocesses or jobs
to run.
"""

import logging
import time


class DependencyError(Exception):
  """Raised when the configuration has an incorrect set of dependencies."""


class BaseAction(object):

  @classmethod
  def able_to_act(cls, component, components, remaining):
    pass

  @classmethod
  def do_action(cls, component):
    pass

  @classmethod
  def get_unfinished(cls, component, remaining):
    pass


class StartAction(BaseAction):
  """Starts components."""

  @classmethod
  def able_to_act(cls, component, components, remaining):
    # A component can start if none of its dependencies are remaining
    return not set(component.dependencies).intersection(remaining)

  @classmethod
  def do_action(cls, component):
    component.start()

  @classmethod
  def get_unfinished(cls, available):
    return [x.name for x in available if not x.is_up()]


class StopAction(BaseAction):
  """Stops components."""

  @classmethod
  def able_to_act(cls, component, components, remaining):
    # A component can stop if there are no remaining components have it as a
    # dependency
    return not [a.name for a in components
                if component.name in a.dependencies and a.name in remaining]

  @classmethod
  def do_action(cls, component):
    component.stop()

  @classmethod
  def get_unfinished(cls, available):
    return [x.name for x in available if not x.is_down()]


class ComponentGroup(object):
  """A grouping of components with dependencies that can be executed."""

  def __init__(self):
    self.components = []

  def add_component(self, component):
    self.components.append(component)

  def execute(self, action, subcomponents=None):
    remaining = subcomponents or [x.name for x in self.components]
    while remaining:
      available = [x for x in self.components if x.name in remaining
                   and action.able_to_act(x, self.components, remaining)]
      if not available:
        # This is a cycle, we have remaining tasks but none can run
        raise DependencyError(
            'Cycle detected: remaining components: %s.' % remaining)
      for component in available:
        action.do_action(component)
        remaining.remove(component.name)
      while True:
        unfinished_components = action.get_unfinished(available)
        if not unfinished_components:
          break
        logging.info(
            'Waiting to be finished: %s.', ', '.join(unfinished_components))
        time.sleep(10)


class Sandlet(object):
  """Top-level component of a sandbox.

  Sandlets should be defined in a way to split applications in a logical way.
  """

  def __init__(self, name):
    self.name = name
    self.dependencies = []
    self.components = ComponentGroup()

  def start(self):
    logging.info('Starting sandlet %s.', self.name)
    self.components.execute(StartAction)

  def stop(self):
    logging.info('Stopping sandlet %s.', self.name)
    self.components.execute(StopAction)

  def is_up(self):
    """Whether the component has finished being started."""
    return True

  def is_down(self):
    """Whether the component has finished being stopped."""
    return True


class SandletComponent(object):
  """Entity of a sandlet that encapsulates a process or job."""

  def __init__(self, name, sandbox_name):
    self.name = name
    self.dependencies = []
    self.sandbox_name = sandbox_name

  def start(self):
    logging.info('Starting component %s.', self.name)

  def stop(self):
    logging.info('Stopping component %s.', self.name)

  def is_up(self):
    """Whether the component has finished being started."""
    return True

  def is_down(self):
    """Whether the component has finished being stopped."""
    return True

