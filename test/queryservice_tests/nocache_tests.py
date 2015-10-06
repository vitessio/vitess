import framework
import nocache_cases


class TestNocache(framework.TestCase):
  def test_sqls(self):
    error_count = self.env.run_cases(nocache_cases.cases)
    if error_count != 0:
      self.fail("test_execution errors: %d"%(error_count))
