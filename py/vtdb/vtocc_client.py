import sys
from optparse import OptionParser

from vtdb import vt_occ

_help = """vtocc_client <host:port> <dbname> [-e "<cmd1; cmd2; cmd3; ...>"]"""

if __name__ == '__main__':
  parser = OptionParser()
  parser.add_option("-e", dest="config_command",
                     help="Config Command to Execute", default=None)
  (options, args) = parser.parse_args()

  if len(args) < 2:
    print >> sys.stderr, _help
    sys.exit(1)

  conn = vt_occ.connect(sys.argv[1], 5, dbname=sys.argv[2])
  curs = conn.cursor()

  #execute commands passed with '-e'
  if options.config_command != None:
    commands = options.config_command.split(';')
    for c in commands:
      try:
        cmd = c.strip()
        print cmd
        curs.execute(cmd, {})
        print 'row count:', curs.rowcount, 'session:', conn.session_id, 'trxn:', conn.transaction_id
        if curs.rowcount:
          print 'description:', curs.description
          for i, v in enumerate(curs):
            print i, v
      except Exception as e:
        print str(e)
        sys.exit(1)
    sys.exit(0)

  #interactive mode
  print 'connected'
  try:
    while True:
      line = sys.stdin.readline().strip().rstrip(';')
      if not line:
        continue
      curs.execute(line, {})
      print 'row count:', curs.rowcount, 'session:', conn.session_id, 'trxn:', conn.transaction_id
      if curs.rowcount:
        print 'description:', curs.description
        for i, v in enumerate(curs):
          print i, v
  except KeyboardInterrupt:
    pass
