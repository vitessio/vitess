import framework
import utils

class TestStatus(framework.TestCase):
  def test_status(self):
    port = self.env.port
    self.assertIn('</html>', utils.get_status(port))
