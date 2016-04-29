package zookeeper_test

import (
	"errors"
	. "launchpad.net/gocheck"
	zk "launchpad.net/gozk/zookeeper"
)

func (s *S) TestRetryChangeCreating(c *C) {
	conn, _ := s.init(c)

	err := conn.RetryChange("/test", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL),
		func(data string, stat *zk.Stat) (string, error) {
			c.Assert(data, Equals, "")
			c.Assert(stat, IsNil)
			return "new", nil
		})
	c.Assert(err, IsNil)

	data, stat, err := conn.Get("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, 0)
	c.Assert(data, Equals, "new")

	acl, _, err := conn.ACL("/test")
	c.Assert(err, IsNil)
	c.Assert(acl, DeepEquals, zk.WorldACL(zk.PERM_ALL))
}

func (s *S) TestRetryChangeSetting(c *C) {
	conn, _ := s.init(c)

	_, err := conn.Create("/test", "old", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, IsNil)

	err = conn.RetryChange("/test", zk.EPHEMERAL, []zk.ACL{},
		func(data string, stat *zk.Stat) (string, error) {
			c.Assert(data, Equals, "old")
			c.Assert(stat, NotNil)
			c.Assert(stat.Version(), Equals, 0)
			return "brand new", nil
		})
	c.Assert(err, IsNil)

	data, stat, err := conn.Get("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, 1)
	c.Assert(data, Equals, "brand new")

	// ACL was unchanged by RetryChange().
	acl, _, err := conn.ACL("/test")
	c.Assert(err, IsNil)
	c.Assert(acl, DeepEquals, zk.WorldACL(zk.PERM_ALL))
}

func (s *S) TestRetryChangeUnchangedValueDoesNothing(c *C) {
	conn, _ := s.init(c)

	_, err := conn.Create("/test", "old", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, IsNil)

	err = conn.RetryChange("/test", zk.EPHEMERAL, []zk.ACL{},
		func(data string, stat *zk.Stat) (string, error) {
			c.Assert(data, Equals, "old")
			c.Assert(stat, NotNil)
			c.Assert(stat.Version(), Equals, 0)
			return "old", nil
		})
	c.Assert(err, IsNil)

	data, stat, err := conn.Get("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, 0) // Unchanged!
	c.Assert(data, Equals, "old")
}

func (s *S) TestRetryChangeConflictOnCreate(c *C) {
	conn, _ := s.init(c)

	changeFunc := func(data string, stat *zk.Stat) (string, error) {
		switch data {
		case "":
			c.Assert(stat, IsNil)
			_, err := conn.Create("/test", "conflict", zk.EPHEMERAL,
				zk.WorldACL(zk.PERM_ALL))
			c.Assert(err, IsNil)
			return "<none> => conflict", nil
		case "conflict":
			c.Assert(stat, NotNil)
			c.Assert(stat.Version(), Equals, 0)
			return "conflict => new", nil
		default:
			c.Fatal("Unexpected node data: " + data)
		}
		return "can't happen", nil
	}

	err := conn.RetryChange("/test", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL), changeFunc)
	c.Assert(err, IsNil)

	data, stat, err := conn.Get("/test")
	c.Assert(err, IsNil)
	c.Assert(data, Equals, "conflict => new")
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, 1)
}

func (s *S) TestRetryChangeConflictOnSetDueToChange(c *C) {
	conn, _ := s.init(c)

	_, err := conn.Create("/test", "old", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, IsNil)

	changeFunc := func(data string, stat *zk.Stat) (string, error) {
		switch data {
		case "old":
			c.Assert(stat, NotNil)
			c.Assert(stat.Version(), Equals, 0)
			_, err := conn.Set("/test", "conflict", 0)
			c.Assert(err, IsNil)
			return "old => new", nil
		case "conflict":
			c.Assert(stat, NotNil)
			c.Assert(stat.Version(), Equals, 1)
			return "conflict => new", nil
		default:
			c.Fatal("Unexpected node data: " + data)
		}
		return "can't happen", nil
	}

	err = conn.RetryChange("/test", zk.EPHEMERAL, []zk.ACL{}, changeFunc)
	c.Assert(err, IsNil)

	data, stat, err := conn.Get("/test")
	c.Assert(err, IsNil)
	c.Assert(data, Equals, "conflict => new")
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, 2)
}

func (s *S) TestRetryChangeConflictOnSetDueToDelete(c *C) {
	conn, _ := s.init(c)

	_, err := conn.Create("/test", "old", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL))
	c.Assert(err, IsNil)

	changeFunc := func(data string, stat *zk.Stat) (string, error) {
		switch data {
		case "old":
			c.Assert(stat, NotNil)
			c.Assert(stat.Version(), Equals, 0)
			err := conn.Delete("/test", 0)
			c.Assert(err, IsNil)
			return "old => <deleted>", nil
		case "":
			c.Assert(stat, IsNil)
			return "<deleted> => new", nil
		default:
			c.Fatal("Unexpected node data: " + data)
		}
		return "can't happen", nil
	}

	err = conn.RetryChange("/test", zk.EPHEMERAL, zk.WorldACL(zk.PERM_READ), changeFunc)
	c.Assert(err, IsNil)

	data, stat, err := conn.Get("/test")
	c.Assert(err, IsNil)
	c.Assert(data, Equals, "<deleted> => new")
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, 0)

	// Should be the new ACL.
	acl, _, err := conn.ACL("/test")
	c.Assert(err, IsNil)
	c.Assert(acl, DeepEquals, zk.WorldACL(zk.PERM_READ))
}

func (s *S) TestRetryChangeErrorInCallback(c *C) {
	conn, _ := s.init(c)

	err := conn.RetryChange("/test", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL),
		func(data string, stat *zk.Stat) (string, error) {
			return "don't use this", errors.New("BOOM!")
		})
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "BOOM!")

	stat, err := conn.Exists("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, IsNil)
}

func (s *S) TestRetryChangeFailsReading(c *C) {
	conn, _ := s.init(c)

	// Write only!
	_, err := conn.Create("/test", "old", zk.EPHEMERAL, zk.WorldACL(zk.PERM_WRITE))
	c.Assert(err, IsNil)

	var called bool
	err = conn.RetryChange("/test", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL),
		func(data string, stat *zk.Stat) (string, error) {
			called = true
			return "", nil
		})
	c.Assert(err, NotNil)
	c.Check(zk.IsError(err, zk.ZNOAUTH), Equals, true, Commentf("%v", err))

	stat, err := conn.Exists("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, 0)

	c.Assert(called, Equals, false)
}

func (s *S) TestRetryChangeFailsSetting(c *C) {
	conn, _ := s.init(c)

	// Read only!
	_, err := conn.Create("/test", "old", zk.EPHEMERAL, zk.WorldACL(zk.PERM_READ))
	c.Assert(err, IsNil)

	var called bool
	err = conn.RetryChange("/test", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL),
		func(data string, stat *zk.Stat) (string, error) {
			called = true
			return "", nil
		})
	c.Check(zk.IsError(err, zk.ZNOAUTH), Equals, true, Commentf("%v", err))

	stat, err := conn.Exists("/test")
	c.Assert(err, IsNil)
	c.Assert(stat, NotNil)
	c.Assert(stat.Version(), Equals, 0)

	c.Assert(called, Equals, true)
}

func (s *S) TestRetryChangeFailsCreating(c *C) {
	conn, _ := s.init(c)

	// Read only!
	_, err := conn.Create("/test", "old", zk.EPHEMERAL, zk.WorldACL(zk.PERM_READ))
	c.Assert(err, IsNil)

	var called bool
	err = conn.RetryChange("/test/sub", zk.EPHEMERAL, zk.WorldACL(zk.PERM_ALL),
		func(data string, stat *zk.Stat) (string, error) {
			called = true
			return "", nil
		})
	c.Assert(err, NotNil)
	c.Check(zk.IsError(err, zk.ZNOAUTH), Equals, true, Commentf("%v", err))

	stat, err := conn.Exists("/test/sub")
	c.Assert(err, IsNil)
	c.Assert(stat, IsNil)

	c.Assert(called, Equals, true)
}
