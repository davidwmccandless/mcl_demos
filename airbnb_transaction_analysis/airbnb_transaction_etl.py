# import dependencies
import pandas
import sys
import sqlalchemy
import datetime
import os
import re

# 1. Read in data

# to get the necessary data, 
# a) navigate to https://www.airbnb.com/users/transaction_history and select 'Completed Payouts'
# b) click 'Export CSVs' and select 'Download CSV file'
# c) click 'Upcoming Payouts' next to 'Completed Payouts'
# d) click 'Export CSVs' and select 'Download CSV file'
# e) copy the full filepath of your folder where you downloaded the CSVs. You'll need it for the command line interface (CLI) call.

# mine is:
# `python3 airbnb_transaction_etl.py "/Users/davidmccandless/Downloads/"`
# you would replace that last token (where I have the filepath to the CSVs) with your own filepath
# note that any CSV with a filename like so will be read in: `airbnb_*.csv`

data_files = os.listdir(sys.argv[1]) # looks in the directory provided as argument or last token in CLI call

# giving credit where it's due for the little utility below to read multiple CSVs to one pandas dataframe
# https://pandasninja.com/2019/04/how-to-read-lots-of-csv-files-easily-into-pandas/
def extract_files(filenames):
    regex = re.compile(r'^airbnb.*csv$') # here's the filename to match
    matches = [m for m in map(regex.match, filenames) if m is not None]

    for match in matches:
        fpath = sys.argv[1]+match.group(0) # details re: parsing CLI arguments from https://www.geeksforgeeks.org/command-line-arguments-in-python/
        ti_c = os.path.getctime(fpath) # get file creation epoch timestamp
        c_ti = datetime.datetime.fromtimestamp(ti_c) # convert epoch timestamp to easily legible timestamp

        yield(
            pandas.read_csv(
            fpath
            , header=0
            , parse_dates=['Date','Start Date'] # we'll make sure to parse these as dates
            ).assign(file_created_on_airbnb=c_ti) # assign a constant value of file creation timestamp
        )

df = pandas.concat(extract_files(data_files))

# we'll remove rows that are effectively duplicates
indexType = df[ (df['Type'] == 'Payout') ].index
df.drop(indexType , inplace=True)

# 2. Load data to SQLite database
engine = sqlalchemy.create_engine('sqlite:///save_pandas.db', echo=True)
sqlite_connection = engine.connect()

sqlite_table = "reservation_history"

# first we're going to create the table DDL
df.head(0).copy().to_sql(sqlite_table, sqlite_connection, if_exists='replace')

# now let's insert a load timestamp column
engine.execute("alter table reservation_history add column this_row_load_ts text default current_timestamp;")

# now let's insert our rows
df.to_sql(sqlite_table, sqlite_connection, if_exists='append')

# 3. Load a dimensional date table to the database
sqlite_table = "dim_date"
min_date=df["Start Date"].min() # get minimum start of stay

# get maximum start of stay + maximum length of nights across all stays
max_date=df["Start Date"].max().to_pydatetime() + datetime.timedelta(days=(df["Nights"].max()))
max_date_trunc=max_date.year # get year of max_date

# construct pandas dataframe between min_date and max stay date + end of following year
df_date=pandas.DataFrame({"Date": pandas.date_range(min_date
, datetime.datetime(max_date_trunc+1,12,31))})

# write said pandas dataframe to SQLite database
df_date.to_sql(sqlite_table, sqlite_connection, if_exists='replace')

#4. Export the results of those queries to spreadsheets
sql_files = os.listdir() # looks in the directory provided as argument or last token in CLI call

# giving credit where it's due for the little utility below to read multiple CSVs to one pandas dataframe
# https://pandasninja.com/2019/04/how-to-read-lots-of-csv-files-easily-into-pandas/
def load_files(filenames):
    regex = re.compile(r'.*sql$') # here's the filename to match
    matches = [m for m in map(regex.match, filenames) if m is not None]

    with pandas.ExcelWriter("airbnb_transactions.xlsx") as writer:
        for match in matches:
            # open text file in read mode
            fn=match.group(0)
            text_file = open(fn, "r")
        
            # read whole file to a string
            sql_text = text_file.read()
        
            # close file
            text_file.close()

            # get name of spreadsheet title from filename
            fn=(fn.split('.'))[0]

            # get select results to pandas dataframe + write to Excel
            pandas.read_sql_query(sql_text, sqlite_connection).to_excel(writer, sheet_name=fn, index=False)

load_files(sql_files)

sqlite_connection.close()
