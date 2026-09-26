-- P3 scatter workload shapes for a sharded sbtest keyspace (hash vindex on id).
-- Usage: sysbench ... /home/vt/perf/P3/lua/p3.lua --shape=<name> [--width=N] [--olap=on] [--ps=on] run
--
-- k is skewed around 125000 in sysbench data; for 110000..122000 the density is ~1 row per k value,
-- so "k BETWEEN s AND s+W" returns ~W rows.

sysbench.cmdline.options = {
  tables = {"Number of tables", 4},
  table_size = {"Rows per table", 250000},
  auto_inc = {"ignored", false},
  shape = {"Query shape", "krange"},
  width = {"Range width / list size / rows", 100},
  limit = {"LIMIT for *_lim shapes", 10},
  olap = {"set workload=olap on the connection", false},
}

local shapes = {}

local function tbl()
  return sysbench.rand.uniform(1, sysbench.opt.tables)
end

local function kstart()
  return sysbench.rand.uniform(110000, 122000 - sysbench.opt.width)
end

local function idstart()
  return sysbench.rand.uniform(1, sysbench.opt.table_size - sysbench.opt.width)
end

-- k range scans (scatter, non-vindex column)
shapes.krange = function()
  local s = kstart()
  return string.format("SELECT c FROM sbtest%d WHERE k BETWEEN %d AND %d", tbl(), s, s + sysbench.opt.width - 1)
end
shapes.krange_ord = function()
  local s = kstart()
  return string.format("SELECT c FROM sbtest%d WHERE k BETWEEN %d AND %d ORDER BY k", tbl(), s, s + sysbench.opt.width - 1)
end
shapes.krange_lim = function()
  local s = kstart()
  return string.format("SELECT c FROM sbtest%d WHERE k BETWEEN %d AND %d ORDER BY k LIMIT %d", tbl(), s, s + sysbench.opt.width - 1, sysbench.opt.limit)
end
-- id ranges (sysbench *_ranges shapes; scatter because id is hashed)
shapes.idrange = function()
  local s = idstart()
  return string.format("SELECT c FROM sbtest%d WHERE id BETWEEN %d AND %d", tbl(), s, s + sysbench.opt.width - 1)
end
shapes.idrange_ordc = function()
  local s = idstart()
  return string.format("SELECT c FROM sbtest%d WHERE id BETWEEN %d AND %d ORDER BY c", tbl(), s, s + sysbench.opt.width - 1)
end
shapes.distinct_c = function()
  local s = idstart()
  return string.format("SELECT DISTINCT c FROM sbtest%d WHERE id BETWEEN %d AND %d ORDER BY c", tbl(), s, s + sysbench.opt.width - 1)
end
-- aggregations
shapes.count = function()
  local s = idstart()
  return string.format("SELECT COUNT(*), SUM(k) FROM sbtest%d WHERE id BETWEEN %d AND %d", tbl(), s, s + sysbench.opt.width - 1)
end
shapes.grp_low = function()
  local s = idstart()
  return string.format("SELECT k %% 100 AS g, COUNT(*), SUM(k) FROM sbtest%d WHERE id BETWEEN %d AND %d GROUP BY g", tbl(), s, s + sysbench.opt.width - 1)
end
shapes.grp_high = function()
  local s = idstart()
  return string.format("SELECT k, COUNT(*) FROM sbtest%d WHERE id BETWEEN %d AND %d GROUP BY k", tbl(), s, s + sysbench.opt.width - 1)
end
shapes.grp_c = function()
  local s = idstart()
  return string.format("SELECT LEFT(c, 2) AS g, COUNT(*) FROM sbtest%d WHERE id BETWEEN %d AND %d GROUP BY g", tbl(), s, s + sysbench.opt.width - 1)
end
shapes.distinct_k = function()
  local s = idstart()
  return string.format("SELECT DISTINCT k FROM sbtest%d WHERE id BETWEEN %d AND %d", tbl(), s, s + sysbench.opt.width - 1)
end
shapes.cnt_distinct = function()
  local s = idstart()
  return string.format("SELECT COUNT(DISTINCT k) FROM sbtest%d WHERE id BETWEEN %d AND %d", tbl(), s, s + sysbench.opt.width - 1)
end
shapes.ord_agg_lim = function()
  local s = idstart()
  return string.format("SELECT k %% 1000 AS g, COUNT(*) AS cnt FROM sbtest%d WHERE id BETWEEN %d AND %d GROUP BY g ORDER BY cnt DESC, g LIMIT 10", tbl(), s, s + sysbench.opt.width - 1)
end
-- IN lists on the primary vindex
shapes.inlist = function()
  local ids = {}
  for i = 1, sysbench.opt.width do
    ids[i] = sysbench.rand.uniform(1, sysbench.opt.table_size)
  end
  return string.format("SELECT c FROM sbtest%d WHERE id IN (%s)", tbl(), table.concat(ids, ","))
end
-- multi-row insert spanning shards (upsert on existing ids keeps the table size constant)
shapes.insert = function()
  local vals = {}
  for i = 1, sysbench.opt.width do
    local id = sysbench.rand.uniform(1, sysbench.opt.table_size)
    vals[i] = string.format("(%d,%d,'%s','%s')", id, sysbench.rand.uniform(1, 250000),
      sysbench.rand.string("###########-###########-###########-###########-###########-###########-###########-###########-###########-###########"),
      sysbench.rand.string("###########-###########-###########-###########-###########"))
  end
  return string.format("INSERT INTO sbtest%d (id, k, c, pad) VALUES %s ON DUPLICATE KEY UPDATE c = VALUES(c)", tbl(), table.concat(vals, ","))
end
-- joins
shapes.join_x = function()
  local s = kstart()
  local a = tbl()
  local b = a % sysbench.opt.tables + 1
  return string.format("SELECT a.c, b.c FROM sbtest%d a JOIN sbtest%d b ON a.k = b.k WHERE a.k BETWEEN %d AND %d", a, b, s, s + sysbench.opt.width - 1)
end
shapes.join_push = function()
  local s = kstart()
  local a = tbl()
  local b = a % sysbench.opt.tables + 1
  return string.format("SELECT a.c, b.c FROM sbtest%d a JOIN sbtest%d b ON a.id = b.id WHERE a.k BETWEEN %d AND %d", a, b, s, s + sysbench.opt.width - 1)
end
-- large results
shapes.big = function()
  local s = idstart()
  return string.format("SELECT * FROM sbtest%d WHERE id BETWEEN %d AND %d", tbl(), s, s + sysbench.opt.width - 1)
end
shapes.big_ord = function()
  local s = idstart()
  return string.format("SELECT * FROM sbtest%d WHERE id BETWEEN %d AND %d ORDER BY id", tbl(), s, s + sysbench.opt.width - 1)
end

function thread_init()
  drv = sysbench.sql.driver()
  con = drv:connect()
  if sysbench.opt.olap then
    con:query("SET workload = 'olap'")
  end
  gen = shapes[sysbench.opt.shape]
  if gen == nil then
    error("unknown shape " .. sysbench.opt.shape)
  end
end

function thread_done()
  con:disconnect()
end

function event()
  con:query(gen())
end
