#!/usr/bin/env python

# This is a code generator that converts the python test cases
# into go.
# TODO(sougou): delete after migration.
from queryservice_tests.cases_framework import MultiCase
import queryservice_tests.nocache_cases as source

def main():
  print "\ttestCases := []framework.Testable{"
  for case in source.cases:
    print_case(case, 2)
  print "\t}"

def print_case(case, indent):
  tabs = buildtabs(indent)
  if isinstance(case, basestring):
    print '%sframework.TestQuery("%s"),' % (tabs, case)
  elif isinstance(case, MultiCase):
    print "%s&framework.MultiCase{" %(tabs)
    if case.doc:
      print '%s\tName: "%s",' % (tabs, case.doc)
    print '%s\tCases: []framework.Testable{' %(tabs)
    for subcase in case:
      print_case(subcase, indent+2)
    print "%s\t}," %(tabs)
    print "%s}," %(tabs)
  else:
    print "%s&framework.TestCase{" %(tabs)
    print_details(case, indent+1)
    print "%s}," %(tabs)

def buildtabs(indent):
  tabs = ''
  for i in range(indent):
    tabs += '\t'
  return tabs

def print_details(case, indent):
  tabs = buildtabs(indent)
  if case.doc:
    print '%sName: "%s",' % (tabs, case.doc)
  print '%sQuery: "%s",' % (tabs, case.sql)
  if case.bindings:
    print '%sBindVars: map[string]interface{}{' % (tabs)
    print_bindings(case.bindings, indent+1)
    print "%s}," %(tabs)
  if case.result:
    print '%sResult: [][]string{' % (tabs)
    print_result(case.result, indent+1)
    print "%s}," %(tabs)
  if case.rewritten:
    print '%sRewritten: []string{' % (tabs)
    for v in case.rewritten:
      print'%s\t"%s",' %(tabs, v)
    print "%s}," %(tabs)
  if case.rowcount:
    print '%sRowsAffected: %s,' % (tabs, case.rowcount)
  if not case.query_plan:
    case.query_plan = "PASS_SELECT"
  print '%sPlan: "%s",' % (tabs, case.query_plan)
  if case.cache_table:
    print '%sTable: "%s",' % (tabs, case.cache_table)
    if case.cache_hits:
      print '%sHits: %s,' % (tabs, case.cache_hits)
    if case.cache_misses:
      print '%sMisses: %s,' % (tabs, case.cache_misses)
    if case.cache_absent:
      print '%sAbsent: %s,' % (tabs, case.cache_absent)
    if case.cache_invalidations:
      print '%sInvalidations: %s,' % (tabs, case.cache_invalidations)


def print_bindings(bindings, indent):
  tabs = buildtabs(indent)
  for (k, v) in bindings.items():
    if isinstance(v, basestring):
      print '%s"%s": "%s",' % (tabs, k, v)
    else:
      print '%s"%s": %s,' % (tabs, k, v)

def print_result(result, indent):
  tabs = buildtabs(indent)
  for row in result:
    print '%s{%s},' %(tabs, ", ".join(['"%s"'%v for v in row]))


if __name__ == '__main__':
  main()
