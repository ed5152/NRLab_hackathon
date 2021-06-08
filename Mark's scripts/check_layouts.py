#!/usr/bin/env /usr/local/miniconda3/bin/python3 

import os
import re
import sys
import simplejson
import pymysql
import subprocess

# The file, in JSON format, with database details and credentials.
credential_file = "cred.txt"

# Full path of directory where the spreadsheets are located
layout_dirs = ["/home/criadmin/EXPERIMENTS/EXP00/",
               "/home/criadmin/EXPERIMENTS/EXP05/",
               "/home/criadmin/EXPERIMENTS/EXP10/",
               "/home/criadmin/EXPERIMENTS/EXP15/",
               "/home/criadmin/EXPERIMENTS/EXP20/",
               "/home/criadmin/EXPERIMENTS/EXP25/",
               "/home/criadmin/EXPERIMENTS/EXP30/"]

# Where our layout data lives:
# tools_dir = "/home/criadmin/Nitzan/MySQL/data_JSONs/"

exe_dir = "/home/criadmin/Nitzan/MySQL/work/ASSIGNMENT/"
# JSON_dir = "/var/www/html/wp-content/plugins/rosenfeld-dbase/JSON/"
JSON_dir = "/home/criadmin/Nitzan/MySQL/work/ASSIGNMENT/"

#################################################################################################
def get_JSON_data(json_file):

  with open(json_file) as data_file:
    try:
      data = simplejson.load(data_file)
    except simplejson.scanner.JSONDecodeError:
      print("Failed to load JSON file: " + json_file)
      sys.exit(-1)

  return data

#################################################################################################
def get_dbase_connection(the_file):

  '''We expect a text file with the JSON format:

  {
    "host"  : <host to connect to, e.g. "localhost">,
    "user"  : <MySQL user to connect as, e.g. "mctest">,
    "pass"  : <The user's password, in quotes>,
    "dbase" : <The database to connect to, e.g. "nitzan">
  }
  '''

  dbase_data = get_JSON_data(the_file)

  # Open database connection
  try:
    db = pymysql.connect(host=dbase_data["host"],
                         user=dbase_data["user"],
                         password=dbase_data["pass"],
                         database=dbase_data["dbase"])

    os.environ["DB_HOST"] = dbase_data["host"]
    os.environ["DB_USER"] = dbase_data["user"]
    os.environ["DB_PASSWORD"] = dbase_data["pass"]
    os.environ["DB_NAME"] = dbase_data["dbase"]

  except pymysql.err.OperationalError:
    print("Error in MySQL connection")
    sys.exit(-1)

  return db

#################################################################################################
def get_db_column(db, col, table):

  # Get the allowed values
  with db.cursor() as cur:
    sql = "SELECT " + col + " FROM " + table

    try:
      cur.execute(sql)
    except pymysql.Error as e:
      print("get_db_column Error: '" + sql + "' failed\n")
      sys.exit(-1)

    # Turn output into a list
    allowed_vals = [item[0] for item in cur.fetchall()]

  return allowed_vals

#################################################################################################
def get_mapping(db, the_key, the_val, table):

  res = {}

  # Get the allowed values
  with db.cursor() as cur:
    sql = "SELECT %s, %s FROM %s" % (the_key, the_val, table)

    try:
      cur.execute(sql)
    except pymysql.Error as e:
      print("get_mapping Error: '" + sql + "' failed\n")
      sys.exit(-1)

    result = cur.fetchall()
    for row in result:
      res[row[0]] = row[1]

  return res

#################################################################################################
def main():

  if len(sys.argv) != 3:
    print("usage: %s <layout type> <listing file>" % (sys.argv[0]))
    sys.exit(-1)

  layout_type = sys.argv[1]
  input_filename = sys.argv[2]

  db = get_dbase_connection(credential_file)
  layout_types = get_db_column(db, "layout", "rosenfeld_Has_sequence")
  db.close()

  if layout_type not in layout_types:
    print("Don't recognise layout type (" + layout_type + "). Bye...")
    sys.exit(-1)

  expIDs = [line.rstrip('\n') for line in open(input_filename)]
  file_dict = {}
  num_found = 0
  num_missing = 0

  print("==== Begin list of missing files ===")
  # First check which layout files for this experiment type we can actually locate
  for expid in expIDs:
    found = False
    exp_dir_exists = False
    layout_dir_exists = False

    for d in layout_dirs:
      ff = d + expid 

      if os.path.exists(ff):
        exp_dir_exists = True
      else:
        continue

      ff = ff + '/LAYOUT/'

      if os.path.exists(ff):
        layout_dir_exists = True
      else:
        continue

      ff = ff + expid 
      allowed_names = [ff+'.xlsx', ff+'_LAYOUT.xlsx', ff+'_Layout.xlsx', ff+'_layout.xlsx']
      # Also allow for expid that's missing user initials. Yuck.
      stub = expid.split('_')[0]
      ff = d + expid + '/LAYOUT/' + stub 
      allowed_names = allowed_names + [ff+'_LAYOUT.xlsx', ff+'_Layout.xlsx', ff+'_layout.xlsx', 'layout.xlsx']
      # And also...
      allowed_names = allowed_names + [d + expid + '/LAYOUT/layout.xlsx']

      for name in allowed_names:
        if os.path.exists(name):
          found = True
          num_found += 1
          file_dict[expid] = name
          break

      if found: break

    if not found:
      num_missing += 1
      message = expid + ': '

      if (exp_dir_exists == False):
        message = message + "EXP Directory"
      elif (layout_dir_exists == False):
        message = message + "LAYOUT Directory"
      else:
        message = message + "File"

      print(message+" not found.")

  print("==== End list of missing files ===")
  print("\n   Number of layout files found  = %d" % (num_found))
  print("   Number of layout files missing = %d" % (num_missing))
  print("\n   *** Starting to process the files we did find... *** \n")
  sys.stdout.flush()

  # We found these files, now check that they obey the expected layout file format
  # exe = tools_dir + "try_layout.py"
  exe = exe_dir + "layout_ingest.py"
  json_file = JSON_dir + layout_type + "_layout.json"
  log_filename = "rosenfeld_" + layout_type + "_layout.py-log"

  print("JSON file = " + json_file)
  print("Log file  = " + log_filename)
  failed = 0
  passed = 0

  for expid in file_dict:
    print("Doing " + expid)
    file = file_dict[expid]
    print("  Layout file  = " + file)

    # Use a blocking call
    try:
      p = subprocess.run([exe, json_file, "N", file])
      ret_code = p.returncode

      if (ret_code != 0):
        # Something went wrong with the call
        print("  FAILED: " + expid + ", ret code "+str(ret_code)+".")
        failed += 1
      else:
        print("  PASSED: " + file)
        passed += 1

      os.rename(log_filename, expid + ".txt")
      sys.stdout.flush()

    except subprocess.SubprocessError as err:
      print("ERROR: fork failed for expid "+expid+" with: "+err)

  print("PASSED = %d" % (passed))
  print("FAILED = %d" % (failed))

#################################################################################################

if __name__ == "__main__":
  main()

