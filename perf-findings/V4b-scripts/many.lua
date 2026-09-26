-- Single-row UPDATE on a random table t0001..tNNNN of keyspace many (autocommit), for the many-workflow runs.
sysbench.cmdline.options = {
  ntables = {"number of tables", 1000},
  rows = {"rows per table", 100},
}

function thread_init()
  drv = sysbench.sql.driver()
  con = drv:connect()
end

function thread_done()
  con:disconnect()
end

function event()
  local t = sysbench.rand.uniform(1, sysbench.opt.ntables)
  local id = sysbench.rand.uniform(1, sysbench.opt.rows)
  con:query(string.format("UPDATE t%04d SET k=k+1 WHERE id=%d", t, id))
end
