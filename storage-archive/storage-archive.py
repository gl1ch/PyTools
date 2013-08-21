#!/usr/bin/env python
# Required: pip install beefish pycrypto argparse boto
#
# TODO:
# 1 - Checkpoints to restart scan on failure
# 2 - Database search
# 3 - Restore functionality
import argparse
import sqlite3
import os
import pwd
import tarfile
import binascii
import getpass
from time import gmtime, strftime
from beefish import decrypt, encrypt_file
from boto.glacier.layer1 import Layer1
from boto.glacier.vault import Vault
import boto.glacier.exceptions

# Items to be filtered from file processing
# TODO: Create better filter 
filtered = 'FILTERED FILE NAME'

# Do not change
archive = None

def main():
  global args

  parser = argparse.ArgumentParser(prog='storage-archive.py',description='This utility is intended to perform archive operations to Amazon Glacier.')
  parser.add_argument('-db', '--database', default='GLACIER-ARCHIVE.sqlite', help='Specify the name for the SQLite database')
  parser.add_argument('-ep', '--encpass', default='15', type=int, help='Number of bytes used when generating encryption passwords. (default: %(default)s)')
  parser.add_argument('-sz', '--asize', default='512', type=int, help='Maximum size in MB of an archive. Value must be a power of 2 (default: %(default)s)')
  parser.add_argument('-t', '--test', action='store_true', help='Test the archive operation without uploading. (default: %(default)s)')
  parser.add_argument('-i', '--initdb', action='store_true', help='Create or update a database and manage glacier configuration')
  parser.add_argument('-a', '--archive', help='Specify directory to archive')
  parser.add_argument('-vc', '--vaultc', action='store_true', help='Create a new vault')
  parser.add_argument('-vd', '--vaultd', action='store_true', help='Delete an existing vault')
  parser.add_argument('-vi', '--vaulti', action='store_true', help='Submit an inventory job')
  parser.add_argument('-vl', '--vaultl', action='store_true', help='View the contents of an inventory job')

  args = vars(parser.parse_args())

  for x, y in args.iteritems():
    if x == 'initdb':
      if y == True:
        init_database()
        init_glconfig()
      else:
        pass
    elif x == 'archive' and y is not None:
      if args['initdb'] == True:
        file_proc(y)            
        tar.close()
        enc_archive()
        glacier_mgmt(archive)
      else:
        init_database()
        init_glconfig()
        file_proc(y)
        tar.close()
        enc_archive()
        glacier_mgmt(archive)   
    elif x == 'vaultc':
      if y == True and args['vaultd'] == True:
        print 'Cannot create and delete at the same time'
        break
      elif y == True:
        glacier_vault_create()
      else:
        pass
    elif x == 'vaultd':
      if y == True and args['vaultc'] == True:
        print 'Cannot create and delete at the same time'
        break
      elif y == True:
        glacier_vault_delete()
      else:
        pass
    elif x == 'vaulti':
      if y == True:
        glacier_vault_inv()
      else:
        pass
    elif x == 'vaultl':
      if y == True:
        glacier_vault_inv_out()
      else:
        pass

def error_stamp(x):
  if x == 'upper':
    print 'WARNING ' + str('-') * 113
    print 'An unexpected error has occured. The error message received was:'
  elif x == 'lower':
    print str('-') * 121
  else:
    return

def is_power(num):
  return num != 0 and ((num & (num - 1)) == 0)

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
    c.execute('CREATE TABLE files (ID integer primary key, MTIME integer, SIZE integer, USER text, PATH text, FILE text, EXTENSION text, ARCHIVE text, ENC_PASS text, VAULT_ID text)')
  else:
    # Use existing database
    print('\r')
    print 'Using existing database: ' + database
    conn = sqlite3.connect(database)
    conn.text_factory = str
    c = conn.cursor()
    return

def init_glconfig():
  # Initialize the config database storing and selecting the necessary information to connect to glacier
  global key
  global secret
  global vault
  global region
  global asize
  global gl_id
  gl_id = 0

  timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
  user = getpass.getuser()
  region = 'us-east-1'
  vault = random(20)
  asize = args['asize']

  try:
    c.execute('SELECT * FROM config')
    rows = c.fetchall()
    if len(rows) > 0:
      print('\r')
      print 'Existing Vaults:'
      print '{0:4} {1:60} {2:40}'.format('ID','Vault','Key Name')
      for row in rows:
        print '{0:4} {1:60} {2:40}'.format(row[0], row[6], row[3])
      print('\r')
      gl_id = 0
      gl_id = raw_input('Choose ID number of existing credentials? (default: %s): ' % gl_id) or gl_id
      gl_id = int(gl_id)
      if gl_id != 0:
        try:
          c.execute('SELECT * FROM config WHERE id IS (?)', (gl_id,))
          rows = c.fetchall()
          key = str(rows[0][3])
          secret = rows[0][4]
          region = rows[0][5]
          vault = rows[0][6]
          asize = rows[0][7]
          gl_id = 1
        except:
          print str('-') * 121
          print 'ERROR: That is not a valid choice.'
          print str('-') * 121
          init_glconfig()
    else:
      gl_id = 0
  except sqlite3.OperationalError:
    c.execute('CREATE TABLE config (ID integer primary key, TIMESTAMP text, USER text, KEYID text, SECKEY text, REGION text, VAULT text, ASIZE int)')
    conn.commit()

  if gl_id == 0:
    print 'Amazon Web Services Key and Secret are required to connect to the Glacier'
    key = raw_input('Enter access key ID: ')
    secret = raw_input('Enter Amazon secret access key: ')
    vault = raw_input('Enter vault name (default: %s): ' % vault) or vault
    region = raw_input('Enter region code (default: %s): ' % region) or region
    asize_in = raw_input('Enter a multipart upload size in MB. Value must be a power of 2 (default: %s MB): ' % asize) or asize
    asize = int(asize_in) * 1024 * 1024
    while is_power(asize) == False:
      print 'Sorry, that number is not a power of 2 (2,4,8,16,32,64,128,256,512,etc)'
      asize = asize / 1024 / 1024
      asize_in = raw_input('Enter a multipart upload size in MB. Value must be a power of 2 (default: %s MB): ' % asize) or asize
      asize = int(asize_in) * 1024 * 1024
    c.execute('INSERT INTO config (TIMESTAMP,USER,KEYID,SECKEY,REGION,VAULT,ASIZE) VALUES (?,?,?,?,?,?,?)', (timestamp,user,key,secret,region,vault,asize))
    conn.commit()
    gl_id = 1
  else:
    return

def file_proc(x):
  # Process files in currentdir, add to tar archive and add to a SQLite database
  global archive_enc
  currentdir = os.path.abspath(x)
  filesindir = os.listdir(x)

  for file in filesindir:
    if filtered not in currentdir:
      archive_mgmt()
      fullfile = os.path.join(currentdir, file)
    
      if os.path.isfile(fullfile) == True:
        extension = str.lower(os.path.splitext(fullfile)[1])
        mtime = int(os.path.getmtime(fullfile))
        size = int(os.path.getsize(fullfile))
        path, filename = os.path.split(fullfile)
        try:
          # Do not bomb out if you cant find the UID
          user = pwd.getpwuid(os.stat(fullfile).st_uid).pw_name
        except KeyError:
          user = 'none'
        print 'Processing: ' + filename
        tar.add(fullfile)
        archive_enc = archive + '.enc'
        c.execute('INSERT INTO files(mtime,size,user,path,file,extension,archive) VALUES (?,?,?,?,?,?,?)', (mtime,size,user,path,filename,extension,archive_enc,))
      elif os.path.islink(fullfile) == False:
        file_proc(fullfile)
      else:
        return
  conn.commit()

def random(x):
  # Random name generation
  return binascii.b2a_hex(os.urandom(x))

def archive_mgmt():
  # Open / Rotate Archives
  global archive
  global tar

  if archive == None:
    archive = '%s_%s' % (random(15), 'backup.tar.gz')
    tar = tarfile.open(archive, 'w:gz')
  elif archive != None and os.path.getsize(archive) > args['asize'] * 1024 * 1024:
    tar.close()
    enc_archive()
    glacier_mgmt(archive)
    archive = '%s_%s' % (random(15), 'backup.tar.gz')
    tar = tarfile.open(archive, 'w:gz')
  else:
    return

def enc_archive():
  # Encrypt archives after they are written
  global archive
  enc_pass = random(args['encpass'])
  enc_archive = archive + '.enc' 
  c.execute('UPDATE files SET enc_pass=(?) WHERE archive=(?)', (enc_pass,enc_archive,))
  conn.commit()
  encrypt_file(archive, enc_archive, enc_pass)
  os.remove(archive)

def glacier_mgmt(archive):
  # Glacier upload / download management
  if gl_id == 1:
    # Archive files to Glacier
    glacier_connect = boto.connect_glacier(aws_access_key_id=key, aws_secret_access_key=secret, region_name=region)
    glacier = glacier_connect.get_vault(vault)
    if os.path.isfile(archive_enc) == False:
      print str('-') * 121
      print 'Error: The archive file was not found'
      print('\r') 
      print str('-') * 121
    print 'Uploading archive: ' + archive_enc
    try:
      if args['test'] != True:
        #archive_id = uploader.upload(archive_enc)
        archive_id = glacier.concurrent_create_archive_from_file(archive_enc, archive_enc)
        c.execute('UPDATE files SET vault_id=(?) WHERE archive=(?)', (vault,archive_enc,))
        conn.commit()
        os.remove(archive_enc)
      else:
        c.execute('UPDATE files SET vault_id=(?) WHERE archive=(?)', (vault,archive_enc,))
        conn.commit()
        os.remove(archive_enc)
    except boto.glacier.exceptions.UnexpectedHTTPResponseError as e:
      error_stamp('upper')
      print e
      error_stamp('lower')
      print 'File ' + str(archive_enc) + ' has been retained for later upload.'
      error_stamp('lower')
  elif gl_id == 2:
    #glacier_connect = Layer1(aws_access_key_id=key, aws_secret_access_key=secret, region_name=region)
    #downloader = retreive_archive(glacier_connect, vault, asize)
    print 'Restore functionality goes here..... Eventually.'

def glacier_vault_create():
  # Create Glacier Vaults 
  init_database()
  init_glconfig()

  print 'Creating vault: ' + vault
  glacier_connect = Layer1(aws_access_key_id=key, aws_secret_access_key=secret, region_name=region)
  glacier_connect.create_vault(vault)

def glacier_vault_delete():
  # Delete Glacier Vaults
  init_database()
  init_glconfig()

  print 'Deleting vault: ' + vault
  glacier_connect = Layer1(aws_access_key_id=key, aws_secret_access_key=secret, region_name=region)
  try:
    glacier_connect.delete_vault(vault)
    c.execute('DELETE FROM config WHERE vault=(?)', (vault,))
    conn.commit()
  except:
    error_stamp('upper')
    print 'Cannot delete vault ' + vault
    print 'It may contain archives that need to be deleted first'
    error_stamp('lower')

def glacier_vault_inv():
  # Submit a vault inventory job
  init_database()
  init_glconfig()
  timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
  user = getpass.getuser()

  try:
    c.execute('SELECT * FROM jobs')
  except sqlite3.OperationalError:
    c.execute('CREATE TABLE jobs (ID integer primary key, TIMESTAMP text, USER text, VAULT text, JOBID text)')
    conn.commit()

  try:
    glacier_connect = Layer1(aws_access_key_id=key, aws_secret_access_key=secret, region_name=region)
    job = glacier_connect.initiate_job(vault, {'Description':'inventory-job', 'Type':'inventory-retrieval', 'Format':'JSON'}) 
    print 'Inventory Job ID: ' + str(job['JobId'])
    c.execute('INSERT INTO jobs (TIMESTAMP,USER,VAULT,JOBID) VALUES (?,?,?,?)', (timestamp,user,vault,job['JobId']))
    conn.commit()
  except boto.glacier.exceptions.UnexpectedHTTPResponseError as e:
    error_stamp('upper')
    print e
    error_stamp('lower')

def glacier_vault_inv_out():
  # Read the inventory of the vault (The job requesting the inventory must be submitted first) 
  init_database()

  try:
    c.execute('SELECT * FROM jobs')
    rows = c.fetchall()
    if len(rows) > 0:
      print('\r')
      print 'Existing Jobs:'
      print '{:<22} {:<15} {:<50} {:<100}'.format('Time Submitted','User','Vault','Amazon Job ID')
      for row in rows:
        print '{:<22} {:<15} {:<50} {:<100}'.format(row[0], row[1], row[2], row[3])
      print('\r')
      gl_id = 0
      gl_id = raw_input('Choose a Job to get inventory? (default: %s): ' % gl_id) or gl_id
      gl_id = int(gl_id)

      if gl_id != 0:
        c.execute('SELECT jobs.jobid,jobs.vault,config.keyid,config.seckey,config.region FROM jobs INNER JOIN config ON jobs.vault = config.vault WHERE jobs.id =(?)', (gl_id,))
        x = c.fetchall()
        job = x[0][0]
        vault = x[0][1]
        key = x[0][2]
        secret = x[0][3]
        region = x[0][4]
        try:
          glacier_connect = Layer1(aws_access_key_id=key, aws_secret_access_key=secret, region_name=region)
          get_job = glacier_connect.get_job_output(vault, job)
          list = get_job['ArchiveList']
          for i in list:
            print 'Archive ID: ' + str(i['ArchiveId'])
            print '{:<25} {:<20} {:<100}'.format('Creation Date','Size','Archive')
            print '{:<25} {:<20} {:<100}'.format(i['CreationDate'],i['Size'],i['ArchiveDescription']) + '\n'
        except boto.glacier.exceptions.UnexpectedHTTPResponseError as e:
          error_stamp('upper')
          print e
          error_stamp('lower')
          get_job = glacier_connect.list_jobs(vault, completed=False)
          list = get_job['JobList']
          print 'Listing currently active jobs:'
          print 'Request ID: ' + str(get_job['RequestId']) + '\n'
          for i in list:
            print 'Job ID: ' + str(i['JobId'])
            print 'Status: ' + str(i['CreationDate'])
            print 'Status: ' + str(i['StatusCode']) 
            print 'Vault ARN: ' + str(i['VaultARN'])+ '\n'
          error_stamp('lower')
        gl_id = 1
      else:
        print str('-') * 121
        print 'ERROR: That is not a valid choice.'
        print str('-') * 121
  except:
    print 'An error has occured'
    pass

if __name__ == '__main__':
  main()
