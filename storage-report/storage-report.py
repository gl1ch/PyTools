#!/usr/bin/python
import sys
import os
import pwd
import time
import argparse
import sqlite3

# Items to be filtered from file processing
filtered = '.'

# Global date calculations
currenttime = time.time()
dayinsecond = 86400

def main():
  global args
  global database
  global conn
  global c

  parser = argparse.ArgumentParser(prog='storage-report.py',description='This utility is intended to collect data on a file server and then perform queries against the data set.')
  parser.add_argument('-db', '--database', default='SA.sqlite', help='Supply a name for the SQLite DB to be generated')
  parser.add_argument('-i', '--initdb', action='store_true', help='Manually create a new database')
  parser.add_argument('-DD', '--deldb', action='store_true', help='Clear existing database')
  parser.add_argument('-s', '--scan', help='Begin a scan of the file system. (default: %(default)s)')
  parser.add_argument('-du', '--dedup', action='store_true', help='Find duplicate files that have the same file name and file size')
  parser.add_argument('-o', '--old', action='store_true', help='Show total amount of files / size of files seperated by range of days since last modification')
  parser.add_argument('-l', '--list', help='List full path of files for files older than number of days. Example: All files older than 100 days.')
  parser.add_argument('-e', '--ext', action='store_true', help='Create a report showing counts of file extensions by date range')
  parser.add_argument('-ed', '--extnodate', action='store_true', help='Create a report showing counts of file extensions')
  parser.add_argument('-u', '--user', action='store_true', help='Create a report showing counts of files by user')
  parser.add_argument('-ue', '--usere', help='Create a report showing counts of files by user')
  parser.add_argument('-uep', '--userep', action='store_true', help='Create a report to help us locate directories with large amount of files.')
  parser.add_argument('-a', '--archive', help='Directories that do not contain subdirectories or files more recent than the number of days given. Example: Show me directories that dont have files or folders newer than 100 days.')

  if len(sys.argv) <= 1:
    parser.print_usage() 
    sys.exit(1) 
  else:
    args = vars(parser.parse_args())
  
  for x, y in args.iteritems():
    if x == 'initdb':
      if y == True:
        init_database()
      else:
        pass
    elif x == 'scan':
      if y and args['initdb'] == True:
        init_database()
        FileProc(args['scan'])
      elif y:
        init_database()
        FileProc(args['scan'])
      else:
        pass
    elif x == 'old':
      if y == True:
        init_database()
        FileOldFiles()
      else:
        pass
    elif x == 'list':
      if y:
        try:
          int(y)
          init_database()
          FileByDays(args['list'])
        except:
          print 'Please enter a numerical value'
      else:
        pass 
    elif x == 'ext':
      if y == True:
        init_database()
        FileByExt()
      else:
        pass
    elif x == 'extnodate':
      if y == True:
        init_database()
        FileByExtNoDate()
      else:
        pass
    elif x == 'user':
      if y == True:
        init_database()
        FileByUser()
      else:
        pass
    elif x == 'usere':
      if y and y.isdigit() != True:
        init_database()
        ExtByUser(args['usere'])
      elif y and y.isdigit() == True:
        print 'I am not a number!'
      else:
        pass
    elif x == 'userep':
      if y == True:
        init_database()
        Prompt('EXTPATH')
        ExtPathByUser(user,ext)
      else:
        pass
    elif x == 'dedup':
      if y == True:
        init_database()
        DeDup()
      else:
        pass 
    elif x == 'archive':
      if y:
        init_database()
        FileArchive(args['archive'])
      else:
        pass

def init_database():
  # Initialize a new database or use an existing database
  global database
  global conn
  global c

  database = args['database']

  if not os.path.isfile(database):
    # Create a new database
    conn = sqlite3.connect(database)
    conn.text_factory = str
    c = conn.cursor()
    print('\r')
    print 'Creating database: ' + database
    sql = 'CREATE TABLE files (ID integer primary key, MTIME integer, ATIME integer, SIZE integer, USER text, PATH text, FILE text, EXTENSION text)'
    c.execute(sql)
  else:
    # Use existing database
    print('\r')
    print 'Using existing database: ' + database
    conn = sqlite3.connect(database)
    conn.text_factory = str
    c = conn.cursor()
    return

def clear_database():
  # Clear the database
  sql = 'DROP TABLE files'
  c.execute(sql)

def Prompt(x):
  if x == 'EXTPATH':
    global user
    global ext
    user = raw_input('Please enter a user name to query against: ')
    ext = raw_input('Please enter an extension to query against (Include the .): ')

def FileProc(currentdir):
  # Process files in currentdir and add to a SQLite database
  currentdir = os.path.abspath(currentdir)
  filesindir = os.listdir(currentdir)

  for file in filesindir:
    if filtered not in currentdir:
      FILE = os.path.join(currentdir, file)

      if os.path.isfile(FILE) == True:
        extension = str.lower(os.path.splitext(FILE)[1])
        mtime = int(os.path.getmtime(FILE))
        atime = int(os.path.getatime(FILE))
        size = int(os.path.getsize(FILE))
        path, filename = os.path.split(FILE)
        try:
          # Do not bomb out if you cant find the UID
          user = pwd.getpwuid(os.stat(FILE).st_uid).pw_name
        except KeyError:
          user = 'none'
        print 'Processing: ' + filename 
        c.execute('INSERT INTO files(mtime,atime,size,user,path,file,extension) VALUES (?,?,?,?,?,?,?)', (mtime,atime,size,user,path,filename,extension))
      elif os.path.islink(FILE) == False: 
        FileProc(FILE)
    conn.commit()

def FileOldFiles():
  # Query the SQLite database and show amount of files / size of files for a date range 
  global query
  c.execute('SELECT temp.range AS [day range], count(*) AS [number of files], SUM(size) AS [summed size] FROM ('
             'SELECT CASE '
             'WHEN files.mtime > (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 30 THEN "AAA" '
             'WHEN files.mtime > (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 90 AND files.mtime <= (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 30 THEN "BBB" '
             'WHEN files.mtime > (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 182 AND files.mtime <= (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 90 THEN "CCC" '
             'WHEN files.mtime > (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 365 AND files.mtime <= (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 182 THEN "DDD" '
             'WHEN files.mtime > (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 730 AND files.mtime <= (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 547 THEN "FFF" '
             'WHEN files.mtime > (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 1095 AND files.mtime <= (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 730 THEN "GGG" '
             'WHEN files.mtime > (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 1460 AND files.mtime <= (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 1095 THEN "HHH" '
             'WHEN files.mtime > (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 1825 AND files.mtime <= (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 1460 THEN "III" '
             'ELSE "JJJ" END '
             'AS range, size FROM files'
           ') temp GROUP BY temp.range')
  query = c.fetchall()
  Report('FILEOLDFILES')

def FileByDays(age):
  # Print a list of files that are older than the time given
  global query
  age = int(currenttime - (int(age) * dayinsecond))
  c.execute('SELECT path,file FROM files WHERE path NOT IN (SELECT path FROM files WHERE mtime > (?))', (age,))
  query = c.fetchall()
  Report('FILEBYDAYS')

def FileByExt():
  # Create a report of files by extension seperated by date range
  global query
  c.execute('SELECT temp.range AS [day range], count(*) AS [number of files], extension AS [File Extensions], SUM(size) AS [summed size] FROM ('
             'SELECT CASE '
             'WHEN files.mtime > (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 30 THEN "AAA" '
             'WHEN files.mtime > (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 90 AND files.mtime <= (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 30 THEN "BBB" '
             'WHEN files.mtime > (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 182 AND files.mtime <= (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 90 THEN "CCC" '
             'WHEN files.mtime > (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 365 AND files.mtime <= (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 182 THEN "DDD" '
             'WHEN files.mtime > (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 730 AND files.mtime <= (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 547 THEN "FFF" '
             'WHEN files.mtime > (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 1095 AND files.mtime <= (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 730 THEN "GGG" '
             'WHEN files.mtime > (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 1460 AND files.mtime <= (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 1095 THEN "HHH" '
             'WHEN files.mtime > (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 1825 AND files.mtime <= (julianday("now") - 2440587.5)*86400.0 - 86400.0 * 1460 THEN "III" '
             'ELSE "JJJ" END '
             'AS range, extension, size FROM files'
           ') temp GROUP BY extension ORDER BY temp.range')
  query = c.fetchall()
  Report('FILEEXT')

def FileByExtNoDate():
  # Create a report of files by extension
  global query
  sql = ('SELECT count(*), extension, SUM(size) FROM files GROUP BY extension ORDER by SUM(size)')
  c.execute(sql)
  query = c.fetchall()
  Report('EXTENSION')

def FileByUser():
  # Create a report of files by extension
  global query
  c.execute('SELECT count(*), user, SUM(size) FROM files GROUP BY user ORDER by SUM(size)')
  query = c.fetchall()
  Report('USER')

def FileArchive(age):
  # Locate directories that are candidates for archival. 
  # We are looking for directories where all files within the directory are older than "age".
  global query
  age = int(currenttime - (int(age) * dayinsecond))
  c.execute('SELECT distinct path FROM files WHERE path NOT IN (SELECT distinct path FROM files WHERE mtime > (?))', (age,))
  query = c.fetchall()
  Report('ARCHIVE')

def ExtByUser(x):
  # See what types of files users are generating
  global query
  c.execute('SELECT count(*), extension, SUM(size) FROM files WHERE user IS (?) GROUP BY extension ORDER by SUM(size)', (x,))
  query = c.fetchall()
  print 'Creating report for user ' + x
  Report('EXTENSION')

def ExtPathByUser(x,y):
  # See what types of files users are generating
  global query
  c.execute('SELECT count(*), extension, SUM(size), path FROM files WHERE user IS (?) AND extension IS (?) GROUP BY path ORDER by SUM(size)', (x,y,))
  query = c.fetchall()
  print 'Creating report for user ' + x
  Report('EXTPATH')

def DeDup():
  # Give me an estimate of duplicate files. This is not perfect as it is only comparing the file name and file size.
  global query
  c.execute('SELECT count(*), file, SUM(size) FROM files GROUP BY file, size HAVING count(*) > 1  ORDER by SUM(size), count(*)')
  query = c.fetchall()
  Report('DEDUP')

def Report(x):
  if x == 'FILEOLDFILES':
    print ''
    print '{0:25} {1:15} {2:20}'.format('Days Old','#','Size of Files')
    print ''
    for col in query:
      size = str(col[2] / 1024) + ' KB'
      numf = str(col[1])
      if col[0] == 'AAA':
        print '{0:25} {1:15} {2:20}'.format('0-30 Days Old',numf,size)
      elif col[0] == 'BBB':
        print '{0:25} {1:15} {2:20}'.format('30-90 Days Old',numf,size)
      elif col[0] == 'CCC':
        print '{0:25} {1:15} {2:20}'.format('90-182 Days Old',numf,size)
      elif col[0] == 'DDD':
        print '{0:25} {1:15} {2:20}'.format('182-365 Days Old',numf,size)
      elif col[0] == 'EEE':
        print '{0:25} {1:15} {2:20}'.format('365-547 Days Old',numf,size)
      elif col[0] == 'FFF':
        print '{0:25} {1:15} {2:20}'.format('547-730 Days Old',numf,size)
      elif col[0] == 'GGG':
        print '{0:25} {1:15} {2:20}'.format('730-1095 Days Old',numf,size)
      elif col[0] == 'HHH':
        print '{0:25} {1:15} {2:20}'.format('1095-1460 Days Old',numf,size)
      elif col[0] == 'III':
        print '{0:25} {1:15} {2:20}'.format('1460-1825 Days Old',numf,size)
      elif col[0] == 'JJJ':
        print '{0:25} {1:15} {2:20}'.format('Over Five Years Old',numf,size)
  elif x == 'FILEBYDAYS':
    for x in query:
      print '{0:60}'.format(x[0] + '/' + x[1])
  elif x == 'FILEEXT':
    print ''
    print '{0:25} {1:13} {2:20} {3:50}'.format('Age of Files','Type','#','File Size')
    print ''
    for col in query:
      numf = str(col[1])
      type = str(col[2])
      size = str(col[3] / 1024) + ' KB'
      if col[0] == 'AAA':
        print '{0:25} {1:13} {2:20} {3:50}'.format('0-30 Days Old',type,numf,size)
      elif col[0] == 'BBB':
        print '{0:25} {1:13} {2:20} {3:50}'.format('30-90 Days Old',type,numf,size)
      elif col[0] == 'CCC':
        print '{0:25} {1:13} {2:20} {3:50}'.format('90-182 Days Old',type,numf,size)
      elif col[0] == 'DDD':
        print '{0:25} {1:13} {2:20} {3:50}'.format('182-365 Days Old',type,numf,size)
      elif col[0] == 'EEE':
        print '{0:25} {1:13} {2:20} {3:50}'.format('365-547 Days Old',type,numf,size)
      elif col[0] == 'FFF':
        print '{0:25} {1:13} {2:20} {3:50}'.format('547-730 Days Old',type,numf,size)
      elif col[0] == 'GGG':
        print '{0:25} {1:13} {2:20} {3:50}'.format('730-1095 Days Old',type,numf,size)
      elif col[0] == 'HHH':
        print '{0:25} {1:13} {2:20} {3:50}'.format('1095-1460 Days Old',type,numf,size)
      elif col[0] == 'III':
        print '{0:25} {1:13} {2:20} {3:50}'.format('1460-1825 Days Old',type,numf,size)
      elif col[0] == 'JJJ':
        print '{0:25} {1:13} {2:20} {3:50}'.format('Over Five Years Old',type,numf,size)
  elif x == 'EXTENSION':
    print ''
    print '{0:25} {1:20} {2:50}'.format('User','#','File Size')
    print ''
    for col in query:
      numf = str(col[0])
      type = str(col[1])
      size = str(col[2] / 1024) + ' KB'
      print '{0:25} {1:20} {2:50}'.format(type, numf, size)
  elif x == 'DEDUP':
    print('\r')
    print '{0:15} {1:25} {2:25} {3:50}'.format('#','File Size','Savings','File Name')
    print('\r')
    tot_numf = 0
    tot_size = 0
    tot_savings = 0
    for col in query:
      numf = col[0]
      file = col[1]
      size = col[2] / 1024
      savings = (size / numf) * (numf - 1)
      tot_numf = tot_numf + (numf - 1)
      tot_size = tot_size + size
      tot_savings = tot_savings + savings
      print '{0:15} {1:25} {2:25} {3:50}'.format(str(numf), str(size) + ' KB', str(savings) + ' KB', file)
    print('\r')
    print 'Totals'
    print '{0:15} {1:25} {2:25}'.format('Files', 'Size', 'Savings')
    print '{0:15} {1:25} {2:25}'.format(str(tot_numf), str(tot_size) + ' KB', str(tot_savings) + ' KB')
  elif x == 'EXTPATH':
    print ''
    print '{0:10} {1:10} {2:20} {3:100}'.format('User','#','File Size','Path')
    print ''
    for col in query:
      numf = str(col[0])
      type = str(col[1])
      size = str(col[2] / 1024) + ' KB'
      path = str(col[3])
      print '{0:10} {1:10} {2:20} {3:100}'.format(type, numf, size, path)
  elif x == 'USER':
    print ''
    print '{0:25} {1:20} {2:50}'.format('User','#','File Size')
    print ''
    for col in query:
      numf = str(col[0])
      type = str(col[1])
      size = str(col[2] / 1024) + ' KB'
      print '{0:25} {1:20} {2:50}'.format(type, numf, size)
  elif x == 'ARCHIVE':
    for col in query:
      print '{0:60}'.format(col[0])
  else:
    pass

if __name__ == "__main__":
  main()
