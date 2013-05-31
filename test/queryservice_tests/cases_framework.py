import json
import re
import urllib2

def cases_iterator(cases):
  for case in cases:
    if isinstance(case, MultiCase):
      for c in case:
        yield c
    else:
      yield case

class Log(object):
  def __init__(self, line):
    self.line = line
    try:
      (self.method,
       self.remote_address,
       self.username,
       self.start_time,
       self.end_time,
       self.total_time,
       self.plan_type,
       self.original_sql,
       self.bind_variables,
       self.number_of_queries,
       self.rewritten_sql,
       self.query_sources,
       self.mysql_response_time,
       self.waiting_for_connection_time,
       self.size_of_response,
       self.cache_hits,
       self.cache_misses,
       self.cache_absent,
       self.cache_invalidations) = line.strip().split('\t')
    except ValueError:
      print "Wrong looking line: %r" % line
      raise

  def check(self, case):

    if isinstance(case, basestring):
      return []

    if isinstance(case, MultiCase):
      return sum((self.check(subcase) for subcase in case.sqls_and_cases), [])

    failures = []

    for method in dir(self):
      if method.startswith('check_'):
        if not case.is_testing_cache and method.startswith('check_cache_'):
          continue
        fail = getattr(self, method)(case)
        if fail:
          failures.append(fail)
    return failures

  def fail(self, reason, should, is_):
    return "FAIL: %s: %r != %r" % (reason, should, is_)

  def check_original_sql(self, case):
    # The following is necessary because Python and Go use different
    # notations for bindings: %(foo)s vs :foo.
    sql = re.sub(r'%\((\w+)\)s', r':\1', case.sql)
    # Eval is a cheap hack - Go always uses doublequotes, Python
    # prefers single quotes.
    if sql != eval(self.original_sql):
      return self.fail('wrong sql', case.sql, self.original_sql)

  def check_cache_hits(self, case):
    if case.cache_hits is not None and int(self.cache_hits) != case.cache_hits:
      return self.fail("Bad Cache Hits", case.cache_hits, self.cache_hits)

  def check_cache_absent(self, case):
    if case.cache_absent is not None and int(self.cache_absent) != case.cache_absent:
      return self.fail("Bad Cache Absent", case.cache_absent, self.cache_absent)

  def check_cache_misses(self, case):
    if case.cache_misses is not None and int(self.cache_misses) != case.cache_misses:
      return self.fail("Bad Cache Misses", case.cache_misses, self.cache_misses)

  def check_cache_invalidations(self, case):
    if case.cache_invalidations is not None and int(self.cache_invalidations) != case.cache_invalidations:
      return self.fail("Bad Cache Invalidations", case.cache_invalidations, self.cache_invalidations)

  ## NOTE(szopa): I am not checking bind variables because I have
  ## trouble parsing them - and I don't want to use a full fledged
  ## JSON encoding on the Go side.
  # def check_bind_variables(self, case):
  #   if self.bind_variables:
  #     bind_variables = json.loads(self.bind_variables)
  #   else:
  #     bind_variables = {}
  #   if bind_variables != case.bindings:
  #     self.fail("Bad bind variables", case.bindings, bind_variables)

  def check_query_plan(self, case):
    if case.query_plan is not None and case.query_plan != self.plan_type:
      return self.fail("Bad query plan", case.query_plan, self.plan_type)

  def check_rewritten_sql(self, case):
    if case.rewritten is None:
      return
    rewritten = '; '.join(case.rewritten)
    if rewritten != self.rewritten_sql:
      self.fail("Bad rewritten SQL", rewritten, self.rewritten_sql)

  # def check_remote_address(self, case):
  #   if not self.remote_address.startswith(case.remote_address):
  #     return self.fail("Bad RemoteAddr", case.remote_address, self.remote_address)

  def check_number_of_queries(self, case):
    if case.rewritten is not None and int(self.number_of_queries) != len(case.rewritten):
      return self.fail("wrong number of queries", len(case.rewritten), int(self.number_of_queries))

class Case(object):
  def __init__(self, sql, bindings=None, result=None, rewritten=None, doc='',
               cache_table="vtocc_cached", query_plan=None, cache_hits=None,
               cache_misses=None, cache_absent=None, cache_invalidations=None,
               remote_address="[::1]"):
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
    self.remote_address = remote_address

  def normalizelog(self, data):
    return [line.split("INFO: ")[-1]
            for line in data.split("\n") if "INFO: " in line]

  def parse_streamlog(self, line):
    line.split('\t')

  @property
  def is_testing_cache(self):
    return any(attr is not None for attr in [self.cache_hits,
                                             self.cache_misses,
                                             self.cache_absent,
                                             self.cache_invalidations])

  def run(self, cursor, querylog=None):
    failures = []
    check_rewritten = self.rewritten is not None and querylog
    if check_rewritten:
      querylog.reset()
    if self.is_testing_cache:
      tstart = self.table_stats()
    if self.sql in ('begin', 'commit', 'rollback'):
      getattr(cursor.connection, self.sql)()
    else:
      cursor.execute(self.sql, self.bindings)
    if self.result is not None:
      result = list(cursor)
      if self.result != result:
        failures.append("%r:\n%s !=\n%s" % (self.sql, self.result, result))
    if check_rewritten:
      rewritten = self.normalizelog(querylog.read())
      if self.rewritten != rewritten:
        failures.append("%r:\n%s !=\n%s" % (self.sql, self.rewritten, rewritten))

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
    return json.load(urllib2.urlopen("http://localhost:9461/debug/table_stats"))[self.cache_table]

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
        if case in ('begin', 'commit', 'rollback'):
          getattr(cursor.connection, case)()
        else:
          cursor.execute(case)
        continue
      failures += case.run(cursor, querylog)
    return failures

  def __iter__(self):
    return iter(self.sqls_and_cases)

  def __str__(self):
    return "MultiCase: %s" % self.doc
