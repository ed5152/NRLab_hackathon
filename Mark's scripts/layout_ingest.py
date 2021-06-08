#!/usr/bin/env /usr/local/miniconda3/bin/python3

# Note: sys.exit(X) will generate the following output status page:
#
# X = 0 ; "SUCCESS" in green.
# X = 98; "WARNING" in orange
# X = 99; "INFO" in blue
# X = <anything else>; "FAILURE" in red.

import sys
import sysconfig

# Are we running in (mini)conda?
if 'conda' not in sysconfig.get_config_var("abs_srcdir"):
  print("--> We don't appear to be running in a (mini)conda environment.")
  print("--> Please try doing 'conda activate' first. Bye.")
  sys.exit(1)

import os
import simplejson
import pandas as pd
import numpy as np
import pymysql
import random
import re
import string
from datetime import datetime
import atexit

# Used to hook any actions we want to perform *if* the ingestion is successful
POST_ACTIONS = {}

# Used for renaming the log file once we know it
TABLE_NAME = ""

#################################################################################################
def tee_error(err_str, log_file):
  ''' Send error to log file and user.'''
  log_file.write(err_str + '\n')
  # print(err_str) 

#################################################################################################
def cleanup(log_dir, log_file, file_name):
  global TABLE_NAME

  log_file.close()
  file_name = log_dir + file_name

  # Have we logged anything?
  log_size = os.path.getsize(file_name)

  if log_size == 0:
    os.remove(file_name)
  else:
    if TABLE_NAME != "":
      canonical_name = log_dir + TABLE_NAME + ".py-log"

      # Append or create the canonically named file
      if os.path.exists(canonical_name):
        log_file = open(canonical_name, 'a+')
        temp_file = open(file_name, 'r')
        log_file.write(temp_file.read())
        log_file.close()
        temp_file.close()
        os.remove(file_name)
      else:
        os.rename(file_name, canonical_name)

#################################################################################################
def get_JSON_data(json_file, log_file):

  try:
    with open(json_file) as data_file:
      try:
        data = simplejson.load(data_file)
      except simplejson.scanner.JSONDecodeError:
        tee_error("Failed to load JSON file: " + json_file, log_file)
        sys.exit(2)
  except FileNotFoundError:
    tee_error("Unable to open JSON file: " + json_file, log_file)
    sys.exit(2)

  return data

#################################################################################################
def get_dbase_connection(log_file):

  # Open database connection
  try:
    db = pymysql.connect(host=os.environ["DB_HOST"],
                         user=os.environ["DB_USER"],
                         password=os.environ["DB_PASSWORD"],
                         database=os.environ["DB_NAME"])
  except Exception:
    log_file.write("Error in MySQL connection\n")
    sys.exit(3)

  return db

#################################################################################################
def check_column_for_nulls2(values, column_label, row_offset, table_name, allowed_null, log_file) :
  # Some columns may allow a string where they can't provide a historical value, e.g. "X",
  # so don't flag this as an error, but instead just turn it silently into a null value.

  idx = row_offset
  num_errors = 0

  for v in values:
    ignore_v = False
    sv = str(v[0])

    if (sv == allowed_null):
      v[0] = ""
    elif (sv == ""):
      tee_error("Missing value2! column: "+column_label+", row: " + str(idx), log_file)
      num_errors += 1

    idx += 1

  return num_errors

#################################################################################################
def check_column_for_nulls(values, column_label, row_offset, table_name, log_file) :

  idx = row_offset
  num_errors = 0

  for v in values:
    if v[0] == "": 
      tee_error("Missing value! column: "+column_label+", row: " + str(idx), log_file)
      num_errors += 1

    idx += 1

  return num_errors

#################################################################################################
def check_validation(ref_data, db, column, col_letter, row, log_file):

  log_file.write("  -- " + ref_data["table"] + "\n")
  num_errors = 0
  validation_vals = get_db_column(db, ref_data["name"], ref_data["table"], log_file)
  idx = 0
  the_list = column.values.tolist()

  for idx in range(len(the_list)):
     val = str(the_list[idx][0])
     if val not in validation_vals:
        num_errors += 1
        tee_error("  Value '" + val + "' from row " + str(idx+row) + ", column " + col_letter + " not validated\n", log_file)
      
  return num_errors

#################################################################################################
def get_last_element(table_name, col_name, db, log_file):
  '''Get the the last value for this column, which must be sorted somehow.'''

  with db.cursor() as cur:
    sql = "SELECT " + col_name + " FROM " + table_name
    sql = sql + " ORDER BY " + col_name + " DESC LIMIT 1"

    try:
      cur.execute(sql)
    except pymysql.Error as e:
      log_file.write("get_last_element Error: '" + sql + "' failed\n")
      sys.exit(5)

    # Turn output into a list
    the_val = [item[0] for item in cur.fetchall()][0]

  return the_val

#################################################################################################
def auto_fill_column(prefix, table_name, col_name, db, col_data, log_file):
  '''We auto fill an entire column, wiping anything that was there before. We
     start from the last entry in the corresponding column in the parent table.'''

  num_elements = len(col_data)
  case_id = get_last_element(table_name, col_name, db, log_file)

  # Could conceivably get an empty value if we're the first entry to exist.
  if (case_id == "" or case_id == None):
    the_num = 0
  else:
    the_num = int(case_id.strip(prefix)) + 1

  df_col_name =  col_data.columns[0]
  the_list = []

  for i in range(num_elements):
    the_list.append(prefix + str(the_num))
    the_num += 1

  # Create a new dataframe from a dictionary
  dict = {df_col_name: the_list}
  col_data = pd.DataFrame(dict)
  return col_data

#################################################################################################
def at_least_one_col(data, the_columns, row, log_file):

  errors = 0

  # Use any column to get the length
  num_elements = len(data[the_columns[0]])

  for idx in range(num_elements):
    found = False

    for the_key in the_columns:
      if ((data[the_key][idx]) != ""):
        found = True
        break

    if not found:
      tee_error("  at_least_one_col: Constraint violation on row " + str(row + idx) + "\n", log_file)
      errors += 1

  return errors

#################################################################################################
def check_regex(data, patterns, row, log_file):

  errors = 0

  # Use any column to get the length
  num_elements = len(data[patterns[0]["column"]])

  for pattern in patterns:
    col = pattern["column"]
    the_regex = pattern["regex"]
    idx = row

    if (("multiple" in pattern) and pattern["multiple"]):
      for val in data[pattern["column"]]:
        # Some cells have multiple entries. Gah.
        tabs = val.split(',')

        for t in tabs:
          t = t.strip()
          if not re.search(the_regex, t):
            log_file.write("Regex error, col="+col+", val="+t+", row = "+str(idx)+"\n")
            errors += 1

        idx += 1
    else:
      for val in data[pattern["column"]]:
        val = val.strip()
        if not re.search(the_regex, val):
          log_file.write("Regex error, col="+col+", val="+val+", row = "+str(idx)+"\n")
          errors += 1

        idx += 1

  return errors

#################################################################################################
def get_db_column(db, col, table, log_file):

  # Get the allowed values
  with db.cursor() as cur:
    sql = "SELECT " + col + " FROM " + table

    try:
      cur.execute(sql)
    except pymysql.Error as e:
      log_file.write("get_db_column Error: '" + sql + "' failed\n")
      sys.exit(5)

    # Turn output into a list
    allowed_vals = [item[0] for item in cur.fetchall()]

  return allowed_vals

#################################################################################################
def warn_consent(excel_data, row, log_file):
  '''Check Confirmed Consent given, otherwise warn but ingest anyway.'''

  consent_col = excel_data["confirmed_consent"]

  for idx in range(len(consent_col)):
    entry = consent_col[idx].strip()
    if entry != "Y": tee_error("   WARNING: Consent not given for row %d.\n" % (row+idx), log_file)

#################################################################################################
def secondary_element(excel_data, action, db, row, log_file):

  column = action["column"]
  separator = action["separator"]
  old_key = action["old_key"]
  errors = 0
  have_multiples = False
  check_vals = False
  data_to_ingest = []

  # This is the column that may have multiple values in each element
  excel_col = excel_data[column]

  # Is there a constraint included?
  if ("check" in action):
    check_vals = True
    allowed_values = get_db_column(db, action["check"]["column"], action["check"]["table"], log_file)

  for idx in range(len(excel_col)):
    e = excel_col[idx].strip()

    if e != "":
      vals = e.split(separator)

      if check_vals:
        for i in range(len(vals)):
          vals[i] = vals[i].strip()
          if vals[i] not in allowed_values:
            errors += 1
            tee_error("   secondary_element: value " + vals[i] + " unknown.\n", log_file)

      if (len(vals) > 1):
        have_multiples = True
        excel_col[idx] = vals[0]

        for s in vals[1:]:
          data_to_ingest.append({"col1":excel_data[old_key][idx], "col2":s})

  if errors > 0:
    log_file.write("ERROR: Too many secondary_element errors ("+str(errors)+").\n")
    sys.exit(-1)
  elif have_multiples:
    if "secondary_element" not in POST_ACTIONS:
      POST_ACTIONS["secondary_element"] = []

    POST_ACTIONS["secondary_element"].append({"table" : action["new_table"],
                                              "column" : action["column"],
                                              "old_key" : action["old_key"],
                                              "col1" : "col1",
                                              "col2" : "col2",
                                              "values" : data_to_ingest })

#################################################################################################
def no_duplicates(excel_data, db, constraint, row, log_file):
  '''Check that the values in a column to be added don't already exist in the target column.
     An example of this would be adding primary key values, etc.'''

  errors = 0
  column = constraint["column"]

  # Get the current values in the database for this table/column
  current_vals = get_db_column(db, constraint["foreign_column"], constraint["foreign_table"], log_file)

  duplicates = []
  lc = len(excel_data[column])
  ec = excel_data[column]

  # First check that the requested entries themselves don't clash
  for i in range(lc-1):
    for j in range(i+1, lc):
      if ec[i] == ec[j]:
        errors += 1
        duplicates.append(entry)

  if errors == 0:
    for entry in ec:
      if entry in current_vals:
        errors += 1
        duplicates.append(entry)

    if errors > 0:
      tee_error("ERROR: the following entries in column '"+column+"' already exist: "+','.join(duplicates), log_file)
  else:
    tee_error("ERROR: the following entries in column '"+column+"' are duplicates: "+','.join(duplicates), log_file)

  return errors

#################################################################################################
def check_FK(excel_data, db, constraint, row, multiple, log_file):
  '''Entries in local "column", of which there may be multiple values separated by "separator",
     must appear in "foreign_column" of "foreign_table".'''

  errors = 0
  column = constraint["column"]
  if multiple ==  True: separator = constraint["separator"]

  # Get the allowed values
  allowed_vals = get_db_column(db, constraint["foreign_column"], constraint["foreign_table"], log_file)

  idx = row-1

  if multiple ==  True:
    for entry in excel_data[column]:
      idx += 1
      if entry == "":
        log_file.write("  WARNING: check_FK empty cell at row " + str(idx) + "\n")
      else:
        to_test = entry.split(separator)
        for tt in to_test:
          tt = tt.strip()
          if tt not in allowed_vals:
            errors += 1
            tee_error("  FK Error (row " +str(idx)+ "): value " + tt + \
                      " not in reference table %s" % (constraint["foreign_table"]), log_file)
  else:
    for entry in excel_data[column]:
      idx += 1
      if entry == "":
        log_file.write("  WARNING: check_FK empty cell at row " + str(idx) + "\n")
      else:
        # Can't have this as an action because it happens too late.
        if (("data_type" in constraint) and (constraint["data_type"] == "string")):
          entry = str(entry)

        try:
          entry = entry.strip()
        except AttributeError as error:
          print("check_FK: Attribute error (column "+column+") when tried to strip " + str(entry))
          print(error)
          errors += 1
          return errors

        if entry not in allowed_vals:
          errors += 1
          tee_error("  FK Error (single) (row " +str(idx)+ "): value " + entry + \
                    " not in reference table %s" % (constraint["foreign_table"]), log_file)

  return errors

#################################################################################################
def check_elements_match(excel_data, file_name, sheet, constraint, log_file):
  '''Needed for qdPCR37K layout files. The expId should be in cell B2 *and* in the 
     filename, but they're often different. Historically we used to just warn of 
     the difference, but with new data we throw an error.'''

  errors = 0
  the_col = constraint["column"]
  the_row = constraint["row"]

  # Get the expid from the filename
  baseN = os.path.basename(file_name)
  fn, ext = os.path.splitext(baseN)
  expid = fn[:fn.find("_layout")]

  df = pd.read_excel(file_name, sheet_name=sheet, usecols=the_col)
  thing = map(lambda x : x[0], df.values)
  value = str(list(thing)[the_row-2])

  # Sanity check
  if expid != value:
    tee_error("ERROR: Experiment ID in file and filename don't match (%s vs. %s)." % (expid, value), log_file)
    errors += 1

  return errors

#################################################################################################
def set_element_as_column(excel_data, the_col, value, log_file):

  # Set the value as an entire column in excel_data. First, get any key from excel_data
  any_key = list(excel_data.keys())[0]
  num_rows = len(excel_data[any_key])
  log_file.write("   set_element_as_column: key is %s, len = %d\n" % (any_key, num_rows))
  excel_data[the_col] = [value] * num_rows

#################################################################################################
def sanitise_expid(excel_data, entry, db, row, log_file):
  '''We shouldn't need this for the new data uploads, as the expIDs should be well formed.
     See the Sample.json file for the historic uploads to see how to use it.'''

  errors = 0
  to_null = entry["to_null"]

  # These are what we're comparing against
  full_expIDs = get_db_column(db, entry["foreign_column"], entry["foreign_table"], log_file)

  # Form a hash of the bare expID with suffixed initials as values
  split_expIDs = {}
  for e in full_expIDs:
    vals = e.split('_')
    split_expIDs[vals[0]] = '_' + '_'.join(vals[1:])

  # Get a reference to the column we may need to modify
  col = excel_data[entry["column"]]

  for idx in range(len(col)):
    e = col[idx]

    if e in to_null:
      col[idx] = None
    else:
      eu = e.upper().strip()

      if e in full_expIDs:
        # A correct value!
        pass
      elif eu in full_expIDs:
        col[idx] = eu
        log_file.write("   Warning: sanitise_expid (row %d): Modified %s to %s\n" % (row+idx, e, col[idx]))
      elif e in split_expIDs:
        # Add the suffix
        col[idx] = col[idx] + split_expIDs[e]
        log_file.write("   Warning: sanitise_expid (row %d): Modified %s to %s\n" % (row+idx, e, col[idx]))
      elif eu in split_expIDs:
        # Add the suffix to uppercased value
        col[idx] = eu + split_expIDs[eu]
        log_file.write("   Warning: sanitise_expid (row %d): Modified %s to %s\n" % (row+idx, e, col[idx]))
      else:
        # It's broke
        log_file.write("   ERROR: sanitise_expid (row %d): Can't modify %s\n" % (row+idx, e))
        errors += 1

  return errors

#################################################################################################
# def convert_timestamp(excel_data, entry, row):
# 
#   the_col = entry["column"]
#   col_ref = excel_data[the_col]
# 
#   for idx in range(len(col_ref)):
#     e = col_ref[idx]
#     if isinstance(e, str):
#       if e == "" : col_ref[idx] = None
#     elif isinstance(e, np.datetime64):
#       t = pd.to_datetime(str(e)) 
#       col_ref[idx] = t.strftime('%Y-%m-%d')
#     else:
#       try:
#         col_ref[idx] = e.strftime('%Y-%m-%d')
#       except Exception as e:
#         tee_error("  ERROR: Couldn't convert timestamp in row %d\n" % (row+idx), log_file)
#         sys.exit(1)
# 
#################################################################################################
def convert_timestamp(excel_data, entry, row):

  from dateutil.parser import parse

  the_col = entry["column"]
  col_ref = excel_data[the_col]

  for idx in range(len(col_ref)):
    e = col_ref[idx]
    if e == "":
      col_ref[idx] = None
    else:
      try:
        dd = parse(str(e), fuzzy=True)
        col_ref[idx] = dd.strftime("%Y-%m-%d")
      except ValueError:
        col_ref[idx] = None
        print("  Warning: timestamp for row %d (%s) could not be converted." % (row+idx, str(e)))

#################################################################################################
def validate_expid(excel_data, entry, db, log_file):
  '''Check that the expids submitted for ingestion have been previously reserved.'''

  num_errors = 0
  submitted_expids = excel_data[entry["column"]]

  if (len(submitted_expids) == 0):
    tee_error("  validate_expid: No expids founds.", log_file)
    return 1

  claimed_expids = []

  # Start by making sure the requested entries have the correct form
  the_regex = entry["regex"]

  for t in submitted_expids:
    # t = t.strip()
    if not re.search(the_regex, t):
      tee_error("  expid has wrong format: " + t + "\n", log_file)
      num_errors += 1

  if (num_errors == 0):
    reserved_expids = get_db_column(db, entry["reservation_column"], entry["reservation_table"], log_file)

    for idx in range(len(submitted_expids)):
      # Pull out the integer part
      e = submitted_expids[idx]
      i = re.search(r'\d+', e).group()

      if i not in reserved_expids:
        tee_error("  expid " + i + " has not been reserved.\n", log_file)
        num_errors += 1
      else:
        claimed_expids.append(i)

  # We need to delete the expids that have been claimed, but only if the data is ingested
  # successfully. Hence, we set up a post action.

  if (num_errors == 0):
    POST_ACTIONS["expid"] = { "table" : entry["reservation_table"],
                              "column" : entry["column"],
                              "values" : claimed_expids }

  return num_errors

#################################################################################################
def accessArray_extras(file_name, sheet, entry, expid, log_file):
  '''Yet another horrible hack, this time to account for AccessArray layout 
     file heterogeneity.'''

  foreign_table = entry["foreign_table"]
  col_letter = entry["column"]

  # Don't need a try/except cluase as we already know this works.
  df = pd.read_excel(file_name, sheet_name=sheet, skiprows=0, usecols=col_letter)
  df.fillna("", inplace=True)
  the_vals = df.values.tolist()
  s = "expid : " + expid + "\n"
  data = {}
  data["expid"] = expid

  for r in entry["rows"]:
    key = r["key"]
    val = r["value"]
    data[key] = str(the_vals[val-2][0])

  POST_ACTIONS["accessArray_extras"] = { "table" : entry["foreign_table"],
                                         "data" : data }

#################################################################################################
def to_str(excel_data, entry, row, log_file):
  '''Cast any numerical (int or float) to a string, as required by MySQL. However, check that
     the value is indeed a suitable numeric.'''

  num_errors = 0
  the_col = excel_data[entry["column"]]

  # These can be any kind of string
  if ("allow_str" in entry) and entry["allow_str"]:
    log_file.write("to_str - have allow_str\n")
    for i in range(len(excel_data[entry["column"]])):

      # Empty values are allowed
      if the_col[i] != "": the_col[i] = str(the_col[i])

  else:
    for i in range(len(excel_data[entry["column"]])):
      if the_col[i] != "":
        v = str(the_col[i])

        try:
          float(v)
          the_col[i] = v
        except ValueError:
          log_file.write("to_str! column: "+entry["column"]+", row: ("+str(row+i)+"), " + v + '\n')
          num_errors += 1

  return num_errors

#################################################################################################
def load_file(file_name, table_name, sheet, row, columns, db, log_file, parse_only,
              constraints=None, actions=None, layout_data=None):

  excel_data = {}
  num_errors = 0

  for column in columns:
    col_letter = column["column"]
    dbase_col_name = column["name"]

    if column["title"] == "N/A":
      try:
        df = pd.read_excel(file_name, sheet_name=sheet, skiprows=row-2, usecols=col_letter, header=None)
      except Exception as e:
        log_file.write("   ERROR: load file: " + str(e) + "\n")            
        sys.exit(9)
    else:
      try:
        df = pd.read_excel(file_name, sheet_name=sheet, skiprows=row-2, usecols=col_letter)
      except Exception as e:
        log_file.write("   ERROR: load file: " + str(e) + "\n")            
        sys.exit(10)

    df.fillna("", inplace=True)

    # Do we need to check against validation values?
    if ("validation" in column):
      log_file.write("Validation needed for column " + col_letter + "\n")
      num_errors += check_validation(column["validation"], db, df, col_letter, row, log_file)

    # Check for missing data that we think should be there
    if (("not_null" in column) and column["not_null"]):
      if ("allowed_null" in column):
        num_errors += check_column_for_nulls2(df.values, col_letter, row, table_name, column["allowed_null"], log_file)
      else:
        num_errors += check_column_for_nulls(df.values, col_letter, row, table_name, log_file)

    # Do we need to autogenerate any values?
    if ("auto_generate" in column) :
      df = auto_fill_column(column["auto_generate"]["prefix"], table_name, column["name"], db, df, log_file)

    thing = map(lambda x : x[0], df.values)
    excel_data[dbase_col_name] = list(thing)

  if excel_data == {}:
    tee_error("No data found Excel file!\n", log_file)
    return None

  if (constraints != None):
    for constraint in constraints:
      constraint_type = constraint["type"]

      if (constraint_type == "single_value"):
        col_letter = constraint["column"]

        if column["title"] == "N/A":
          df = pd.read_excel(file_name, sheet_name=sheet, skiprows=row-2, usecols=col_letter, header=None)
        else:
          df = pd.read_excel(file_name, sheet_name=sheet, skiprows=row-2, usecols=col_letter)

        # Constraint columns should not contain gaps
        df.fillna("", inplace=True)
        num_errors += check_column_for_nulls(df.values, col_letter, row, table_name, log_file)

        deleted = 0
        the_list = df.values.tolist()

        for idx in range(len(the_list)):
          val = the_list[idx][0]
          if val != constraint["value"]:
            for key in excel_data: del excel_data[key][idx-deleted]
            deleted += 1

      elif (constraint_type == "at_least_one_column"):
        num_errors += at_least_one_col(excel_data, constraint["columns"], row, log_file)

      elif (constraint_type == "regex"):
        num_errors += check_regex(excel_data, constraint["patterns"], row, log_file)

      elif (constraint_type == "no_duplicates"):
        num_errors += no_duplicates(excel_data, db, constraint, row, log_file)

      elif (constraint_type == "FK"):
        num_errors += check_FK(excel_data, db, constraint, row, False, log_file)

      elif (constraint_type == "mult_FK"):
        num_errors += check_FK(excel_data, db, constraint, row, True, log_file)

      elif (constraint_type == "check_elements_match"):
        num_errors += check_elements_match(excel_data, file_name, sheet, constraint, log_file)

      elif (constraint_type == "column_length"):
        col_length = len(excel_data[constraint["column"]])

        if (col_length != constraint["value"]):
          num_errors += 1
          tee_error("ERROR: I expected columns of length %d but found %d\n" % (constraint["value"], col_length) , log_file)

      else:
        log_file.write("Unknown constraint type: " + constraint_type + "\n")
        sys.exit(1)

  if (actions != None):
    for entry in actions:
      log_file.write("  Require action '"+entry["action"]+"' for column "+entry["column"]+"\n")
      if num_errors > 0: log_file.write("  Already have errors.\n")

      if entry["action"] == "to_str":
        # the_col = excel_data[entry["column"]]
        # for i in range(len(excel_data[entry["column"]])): the_col[i] = str(the_col[i])
        num_errors += to_str(excel_data, entry, row, log_file)
      elif entry["action"] == "secondary_element":
        secondary_element(excel_data, entry, db, row, log_file)
      elif entry["action"] == "warn_consent":
        warn_consent(excel_data, row, log_file)
      elif entry["action"] == "sanitise_expid":
        num_errors += sanitise_expid(excel_data, entry, db, row, log_file)
      elif entry["action"] == "validate_expid":
        num_errors += validate_expid(excel_data, entry, db, log_file)
      elif entry["action"] == "convert_timestamp":
        convert_timestamp(excel_data, entry, row)
      elif entry["action"] == "accessArray_extras":
        accessArray_extras(file_name, sheet, entry, layout_data["expid"], log_file)
      else:
        log_file.write("  --> Unknown action '"+entry["action"]+"' requested! Bye.\n")
        sys.exit(1)

  # With layouts we need to add the expID which we got from the filename. Use any of the
  # current columns to get the column length.
  if (layout_data != None):
    set_element_as_column(excel_data, layout_data["column"], layout_data["expid"], log_file)

  return excel_data, num_errors

#################################################################################################
def merge_compound(compound_data, table_data):

  for r in compound_data:
    separator = r["separator"]
    dbase_row = r["dbase_row"]
    element_lst = r["element_lst"]
    value = separator.join(r["element_lst"])
    table_data[dbase_row] = []

    for idx in range(len(table_data[element_lst[0]])):
      ll = []
      for el in element_lst: ll.append(table_data[el][idx])
      value = separator.join(ll)
      table_data[dbase_row].append(value)

  return table_data

#################################################################################################
def check_for_valid_layout(excel_file, TABLE_NAME, layout, db, log_file):
  '''Layout files must be EXPXXX_YYY_layout.xlsx, i.e. a valid experiment ID. However, this
     will have been checked for in the PHP front end. The front end also checks that the
     expID embedded in the filename actually exists.'''

  # Get a list of allowed layouts. This should also have been checked for.
  layouts = get_db_column(db, "layout", "rosenfeld_Has_sequence", log_file)

  if layout["type"] not in layouts:
    tee_error("  ERROR: Unknown layout ("+layout["type"]+")\n", log_file)
    sys.exit(13)

  # We'll pass the expID in case it's of use.
  base_name = os.path.basename(excel_file)
  layout["expid"] = base_name[:base_name.find("_layout")]

  # Check whether expid already exists in the layout table
  expIDs = get_db_column(db, "expid", TABLE_NAME, log_file)

  if (layout["expid"] in expIDs):
    tee_error("  ERROR: This expid ("+layout["expid"]+") already exists for this layout.\n", log_file)
    sys.exit(13)

#################################################################################################
def parse_data(excel_file, json_record, db, parse_only, log_file):
  '''
   Parse the JSON data, and the file should have the format:

  {
    "table" : <table name>,                                -- e.g. "Researcher",
    "data" : 
    [
      {
         "file_name" : <Excel file to look in>,            -- e.g. "users.xlsx"
         "sheet" :  <The sheet to look in>,                -- e.g. "Sheet1"
         "row": <row number in Excel sheet, as integer>    -- e.g. 2   
         "compound" :                                      -- if present, a list of compound values
         [
          {
            "dbase_row" : <row entry in the dbase table>                  -- e.g. "Name"
            "element_lst" : <list of entries to join to make dbase_row",  -- e.g. ["first_name", "last_name"]
            "separator" : <separator for entries in element_lst>          -- e.g. " "
          }
         ],
         "constraint" : <if present, any constraint>  -- e.g. ["E", "regular_user"], 
         {
           -- NOTE: if a constraint is defined then its entries *must* be present.
           "type" : <the type of constraint. One of "single_value", "at_least_one_column">

           -- Depending on the value of "type", we expect the following entries at the same level:
           -- "type" : "single_value"
           "column" : <the column with the necessary constraint>          -- e.g. "E"
           "value" : <the necessary value for this record>                -- e.g. "regular_user"

           -- "type" : "at_least_one_column:
           "columns" : [ <list of dbase column names to compare, i.e. *not* the column names in Excel>]
                     -- e.g. [ "externalRef", "externalAliquoteid", "referenceSample" ]
         },
         "data": [                                        -- a list of columns to read
            {
              "column": <the column>,                     -- e.g. "A"
              "title": <the column title, or N/A>,        -- e.g."First Name(s)"
              "name": <the name in dbase table, or what to reference in compound->element_lst>,
                                                          -- e.g. "first_name"
              "insert" : <optional; whether to put straight into dbase table. Default is true>,
                                                          -- e.g. false
              "not_null" : <optional; whether a value is required. Default is false>,
                                                          -- e.g. true
              "validation" : <optional, whether this column's values are validated via another table>
              {
                "table" : <the table we validate from>             -- e.g. "cancer_subtype",
                "name" :  <the row name in the validation table>   -- e.g. "subtype"
              }
            },
         ... other column records ...
        ]
      },
    ... other file records ...
    ]
  }
  '''

  global TABLE_NAME

  the_file = json_record["data"][0]
  TABLE_NAME = json_record["table"]
  log_file.write("Table is: " + TABLE_NAME + '\n')

  if not os.path.exists(excel_file):
    log_file.write("File " + excel_file + " does not seem to exist.\n")
    sys.exit(1)

  have_constraint = True if ("constraint" in the_file) else False
  have_compound   = True if ("compound" in the_file) else False
  have_actions    = True if ("modify" in the_file) else False
  have_layout     = True if ("layout" in the_file) else False

  if have_layout == True: 
    check_for_valid_layout(excel_file, TABLE_NAME, the_file["layout"], db, log_file)

  table_data, num_errors = load_file(excel_file,
                                     json_record["table"],
                                     the_file["sheet"], 
                                     the_file["row"],
                                     the_file["data"],
                                     db,
                                     log_file,
                                     parse_only,
                                     the_file["constraint"] if have_constraint else None,
                                     the_file["modify"] if have_actions else None,
                                     the_file["layout"] if have_layout else None)

  if (num_errors > 0):
    err_str = "%d errors found in %s. Baling...\n" % (num_errors, os.path.basename(excel_file))
    tee_error(err_str, log_file)
    sys.exit(1)

  if have_compound: 
    table_data = merge_compound(the_file["compound"], table_data)

  # Can now safely delete any data not marked for insertion.
  for e in the_file["data"]:
    if ("insert" in e) and not e["insert"]:
      del table_data[e["name"]]

  return table_data

#################################################################################################
def my_execute(cursor, sql_query, table, key, values, log_file):

  try:
    if values == None:
      cursor.execute(sql_query)
    else:
      cursor.execute(sql_query, tuple(values))
  except pymysql.Error as e:
    tee_error("MySQL Error in table " + table + ", idx/key=%s, %d: %s\n" % (key, e.args[0], e.args[1]), log_file)
    tee_error(" -> SQL was: %s\n" % (sql_query), log_file)
    tee_error(" -> values were: " + str(values) + "\n", log_file)
    sys.exit(16)
  except Exception as ex:
    tee_error("Error (sql = " + sql_query + "), where idx/key=%s. Exception: %s\n" % (key, ex.__class__.__name__), log_file)
    tee_error("  -> values were: " + str(values) + "\n", log_file)
    sys.exit(17)

#################################################################################################
def ingest_data(data, table, db, log_file):

  the_keys = list(data.keys())
  line = ", ".join(the_keys)
  sql_query = "INSERT INTO " + table + " (" + line + ") VALUES (" + ','.join(["%s"]*len(the_keys)) + ")"
  cursor = db.cursor()
  num_rows = len(data[the_keys[0]])

  if (num_rows == 0):
    tee_error("  ingest_data: Something went wrong. num_rows (keyed off %s) is zero\n" % (the_keys[0]), log_file)
    return

  log_file.write("  ingest_data: num_rows = %d (keyed off column %s)\n" % (num_rows, the_keys[0]))

  for idx in range(num_rows):
    values = []

    for k in the_keys:
      # Put NULL values for empty strings
      try:
        val = None if (data[k][idx] == "") else data[k][idx]
        values.append(val)
      except IndexError:
        log_file.write("  ingest_data: Failed for key %s, idx = %d\n" % (k, idx))
        log_file.write("             : Column %s has %d rows.\n" % (k, len(data[k])))
        sys.exit(11)

    my_execute(cursor, sql_query, table, str(idx), values, log_file)

  # Looks like we've executed the above successfully. Now we need to perform any post actions
  # that were requested.

  for key in POST_ACTIONS:
    if key == "expid":
      # Delete the reserved expid(s)
      table = POST_ACTIONS["expid"]["table"]
      column = POST_ACTIONS["expid"]["column"]
      values = POST_ACTIONS["expid"]["values"]
      sql_query = "DELETE FROM " + table + " WHERE " + column + " IN ("
      sql_query = sql_query + ",".join(values) + ")"
      my_execute(cursor, sql_query, table, key, None, log_file)

    elif key == "secondary_element":
      for record in POST_ACTIONS[key]:
        table = record["table"]
        column = record["column"]
        old_key = record["old_key"]
        values = record["values"]
        col1 = record["col1"]
        col2 = record["col2"]
        sql_query = "INSERT INTO " + table + " (" + old_key + ", " + column + ") VALUES (%s, %s)"

        for datum in values:
          my_execute(cursor, sql_query, table, key, [datum[col1], datum[col2]], log_file)

    elif key == "accessArray_extras":
      table = POST_ACTIONS["accessArray_extras"]["table"]
      expid = POST_ACTIONS["accessArray_extras"]["data"]["expid"]
      the_keys = list(POST_ACTIONS["accessArray_extras"]["data"].keys())
      the_vals = list(POST_ACTIONS["accessArray_extras"]["data"].values())

      sql_query = "INSERT INTO " + table + " (" + ", ".join(the_keys) + ") VALUES (" + \
                   ','.join(["%s"]*len(the_keys)) + ")"
      my_execute(cursor, sql_query, table, expid, the_vals, log_file)

    else:
      log_file.write("  ERROR: Whoa! Unknown POST action: " + key + "\n")

  db.commit()

#################################################################################################
def main():

  # argv[1]: The Excel file to parse.
  # argv[2]: Ingest [Y/N]
  # argv[3]: JSON metadata file with table <-> spreadsheet details.

  if (len(sys.argv) != 4):
    print("usage: %s <JSON metadata file> <Ingest [Y/N]> <Excel file>" % (sys.argv[0]))
    sys.exit(18)

  log_dir = './'
  log_file_name = "".join(random.sample(string.ascii_letters+string.digits, 8))

  try:
    log_file = open(log_dir + log_file_name, 'a+')
  except IOError as err:
    print("Could not open log file: {0}".format(err))
    sys.exit(19)

  atexit.register(cleanup, log_dir, log_file, log_file_name)
  # parse_only = True if (sys.argv[2] != "Y") else False
  parse_only = True

  json_data = get_JSON_data(sys.argv[1], log_file)
  db = get_dbase_connection(log_file)
  file_data = parse_data(sys.argv[3], json_data, db, parse_only, log_file)

  if (parse_only):
    log_file.write("Didn't get 'Y' for ingest option, so just parsing data.\n")
  else:
    ingest_data(file_data, json_data['table'], db, log_file)

  db.close()

#################################################################################################

if __name__ == "__main__":
  main()

