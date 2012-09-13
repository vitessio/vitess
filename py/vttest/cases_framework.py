import json
import urllib2

class Case(object):
  def __init__(self, sql, bindings=None, result=None, rewritten=[], doc='',
               cache_table="vtocc_cached", query_plan=None, cache_hits=None,
               cache_misses=None, cache_absent=None, cache_invalidations=None):
    # For all cache_* parameters, a number n means "check this value
    # is exactly n," while None means "I am not interested in this
    # value, leave it alone."
    self.sql = sql
    self.bindings = bindings or {}
    self.result = result
    if isinstance(rewritten, basestring):
      rewritten = [rewritten]
    self.rewritten = rewritten
    self.doc = doc
    self.query_plan = query_plan
    self.cache_table = cache_table
    self.cache_hits= cache_hits
    self.cache_misses = cache_misses
    self.cache_absent = cache_absent
    self.cache_invalidations = cache_invalidations

  def normalizelog(self, data):
    return [line.split("INFO: ")[-1]
            for line in data.split("\n") if "INFO: " in line]

  @property
  def is_testing_cache(self):
    return any(attr is not None for attr in [self.cache_hits,
                                             self.cache_misses,
                                             self.cache_absent,
                                             self.cache_invalidations])

  def run(self, cursor, querylog=None):
    failures = []
    check_rewritten = self.rewritten and querylog
    if check_rewritten:
      querylog.reset()
    if self.is_testing_cache:
      tstart = self.table_stats()
    cursor.execute(self.sql, self.bindings)
    if self.result:
      result = list(cursor)
      if self.result != result:
        failures.append("%r: %s != %s" % (self.sql, self.result, result))
    if check_rewritten:
      rewritten = self.normalizelog(querylog.read())
      if self.rewritten != rewritten:
        failures.append("%r: %s != %s" % (self.sql, self.rewritten, rewritten))

    if self.is_testing_cache:
      tdelta = self.table_stats_delta(tstart)
      if self.cache_hits is not None and tdelta['Hits'] != self.cache_hits:
        failures.append("Bad Cache Hits: %s != %s" % (self.cache_hits, tdelta['Hits']))

      if self.cache_absent is not None and tdelta['Absent'] != self.cache_absent:
        failures.append("Bad Cache Absent: %s != %s" % (self.cache_absent, tdelta['Absent']))

      if self.cache_misses is not None and tdelta['Misses'] != self.cache_misses:
        failures.append("Bad Cache Misses: %s != %s" % (self.cache_misses, tdelta['Misses']))

      if self.cache_invalidations is not None and tdelta['Invalidations'] != self.cache_invalidations:
        failures.append("Bad Cache Invalidations: %s != %s" % (self.cache_invalidations, tdelta['Invalidations']))


    return failures

  def table_stats_delta(self, old):
    result = {}
    new = self.table_stats()
    for k, v in new.items():
      result[k] = new[k] - old[k]
    return result

  def table_stats(self):
    return json.load(urllib2.urlopen("http://localhost:9461/debug/schema/tables"))[self.cache_table]

  def __str__(self):
    return "Case %r" % self.doc

class MultiCase(object):
  def __init__(self, doc, sqls_and_cases):
    self.doc = doc
    self.sqls_and_cases = sqls_and_cases

  def run(self, cursor, querylog=None):
    failures = []
    for case in self.sqls_and_cases:
      if isinstance(case, basestring):
        cursor.execute(case)
        continue
      failures += case.run(cursor, querylog)
    return failures

  def __str__(self):
    return "MultiCase: %s" % self.doc
