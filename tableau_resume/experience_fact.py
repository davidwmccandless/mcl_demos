# import dependencies
from google.colab import auth
import gspread
from google.auth import default
import pandas as pd
import sqlalchemy
import os
from datetime import datetime

# authenticating to google
auth.authenticate_user()
creds, _ = default()
gc = gspread.authorize(creds)

# delete SQLite database if exists already e.g. if you're running this Colab iteratively
fp="save_pandas.db"
if os.path.exists(fp):
  os.remove(fp)

# define engine and connection
engine = sqlalchemy.create_engine('sqlite:///save_pandas.db', echo=True)
sqlite_connection = engine.connect()

# define function to write all Google sheets to table in SQLite
def gsheet_to_sqlite(ws, eng, conn):

  # iterate over sheets
  for i in ws.worksheets():

    # get worksheet title
    t=i.title

    # get ith worksheet
    worksheet = ws.worksheet(t)

    # get_all_values gives a list of rows
    rows = worksheet.get_all_values()

    # Convert to a DataFrame
    df = pd.DataFrame(rows)

    # set column names equal to values in row index position 0
    df.columns = df.iloc[0]

    # remove first row from DataFrame
    df = df[1:]

    # write to SQLite, using sheet name as table name
    df.to_sql(t, conn, if_exists='replace')

# defining my worksheet
worksheet_list = gc.open('experiences')

# execute function to write all sheets to table in SQLite
gsheet_to_sqlite(worksheet_list, engine, sqlite_connection)

# create experience_fact SQLite table
sql_statement="""
create table experience_fact as
select
"Resource Name"
,"Tool Role(s)"
,"Resource Title"
,"Begin Date"
,"End Date"
,"Commits"
,"Used VCS"
,b.company_id
,c.*
from
dim_role a
inner join dim_company b
on a.company_id=b.company_id
inner join dim_tool c
on c.tool_id=a.tool_id;
"""

# execute the above SQL
engine.execute(sql_statement)

sql_statement='select * from experience_fact;'

# read SQLite table to csv
pd.read_sql_query(sql_statement, sqlite_connection).to_csv('experience_report.csv', index=True)

# get minimum and maximum dates from experience report
sql_statement="""
select
"Begin Date" as min_date
,"End Date" as max_date
from
dim_role
group by 1,2
"""

# execute the above SQL
dates=pd.read_sql_query(sql_statement, sqlite_connection)

# cast dates in m/d/yy or m/d/yyyy format
def date_caster(d):
  date_split=d.split('/')
  y=int(date_split[2])
  if y<2000:
    y=y+2000
  datetime_object=datetime(y, int(date_split[0]), int(date_split[1]))
  return datetime.strftime(datetime_object,"%Y-%m-%d")

# get unique "Begin Date" as list
min_date_list=dates['min_date'].unique().tolist()

# cast string to date
min_dates_casted = [date_caster(i) for i in min_date_list]

# get unique "End Date" as list
max_date_list=dates['max_date'].unique().tolist()

# cast string to date
max_dates_casted = [date_caster(i) for i in max_date_list]

# if you need refreshed dim_date
df = pd.DataFrame({"date_dt": pd.date_range(min(min_dates_casted), max(max_dates_casted))})

df.to_csv('dim_date.csv', index=False)
