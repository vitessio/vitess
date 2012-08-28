from vtdb import cursor

import framework

class TestStream(framework.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def set_env(self, env):
    self.env = env

# more fields!

  def test_basic_stream(self):
    # insert 100 rows in a table
    self.env.execute("begin")
    for i in xrange(100):
      self.env.execute("insert into vtocc_big values " +
                       "(" + str(i) + ", " +
                       "'AAAAAAAAAAAAAAAAAA " + str(i) + "', " +
                       "'BBBBBBBBBBBBBBBBBB " + str(i) + "', " +
                       "'C', " +
                       "'DDDDDDDDDDDDDDDDDD " + str(i) + "', " +
                       "'EEEEEEEEEEEEEEEEEE " + str(i) + "', " +
                       "now()," +
                       "'FFFFFFFFFFFFFFFFFF " + str(i) + "', " +
                       "'GGGGGGGGGGGGGGGGGG " + str(i) + "', " +
                       str(i) + ", " +
                       str(i) + ", " +
                       "now()," +
                       str(i) + ", " +
                       str(i%100) + ")")
    self.env.execute("commit")

    loop_count = 1

    # select lots of data using a non-streaming query
    if True:
      for i in xrange(loop_count):
        cu = self.env.execute("select * from vtocc_big b1, vtocc_big b2")
        self.assertEqual(len(cu.fetchall()), 10000)

    # select lots of data using a streaming query
    if True:
      for i in xrange(loop_count):
        cu = self.env.execute("select * from vtocc_big b1, vtocc_big b2",
                              cursorclass=cursor.StreamCursor)
        count = 0
        while cu.fetchone() != None:
          count += 1
        self.assertEqual(count, 10000)
