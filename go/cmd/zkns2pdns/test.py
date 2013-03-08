#!/usr/bin/python

import sys

def main(args):
  print 'HELO\t2'
  if args:
    for arg in args:
      print "Q\t%s\tIN\tANY\t-1\t1.1.1.1\t1.1.1.2" % arg
  else:
    print "Q\ttest.zkns.zk\tIN\tANY\t-1\t1.1.1.1\t1.1.1.2"

if __name__ == '__main__':
  main(sys.argv[1:])
