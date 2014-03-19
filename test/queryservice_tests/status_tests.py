import framework
import utils

class TestStatus(framework.TestCase):
  def test_status(self):
    try:
      port = self.env.tablet.port
    except AttributeError:
      port = self.env.vtoccport
    self.assertIn('</html>', utils.get_status(port))
