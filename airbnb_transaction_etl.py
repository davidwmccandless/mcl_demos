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
def load_files(filenames):
    regex = re.compile(r'^airbnb.*csv$') # here's the filename to match
    matches = [m for m in map(regex.match, filenames) if m is not None]

    for match in matches:
        print(matches)
        yield(
            pandas.read_csv(
            sys.argv[1]+match.group(0)
            , header=0
            , parse_dates=['Date','Start Date'] # we'll make sure to parse these as dates
            ) # details re: parsing CLI arguments from https://www.geeksforgeeks.org/command-line-arguments-in-python/
        )

df = pandas.concat(load_files(data_files))

# we'll remove rows that are effectively duplicates
indexType = df[ (df['Type'] == 'Payout') ].index
df.drop(indexType , inplace=True)

# 2. Load data to SQLite database
engine = sqlalchemy.create_engine('sqlite:///save_pandas.db', echo=True)
sqlite_connection = engine.connect()

sqlite_table = "reservation_history"
df.to_sql(sqlite_table, sqlite_connection, if_exists='replace')

# 3. Load a dimensional date table to the database
sqlite_table = "dim_date"
min_date=df["Start Date"].min() # get minimum start of stay
print(min_date)
print(type(min_date))
max_date=df["Start Date"].max().to_pydatetime() + datetime.timedelta(days=(df["Nights"].max())) # get maximum start of stay + maximum length of nights across all stays
max_date_trunc=max_date.year # get year of max_date

df_date=pandas.DataFrame({"Date": pandas.date_range(min_date
, datetime.datetime(max_date_trunc+1,12,31))}) # add a year

df_date.to_sql(sqlite_table, sqlite_connection, if_exists='replace')

#4. Export the results of those queries to spreadsheets
query="""
/*
using the CTEs below we protect our guests' data privacy + calculate a few metrics that would be quite tedious to
calculate via Tableau
*/
with rn_pre as
(
    select
    Guest
    ,Listing
    ,count(distinct "Confirmation Code") as rc
    ,min("Start Date") as msd
    ,row_number() over () as "Guest ID"
    ,row_number() over (partition by Listing order by count(*) desc) as rn2
    from
    reservation_history
    where Guest is not null
    group by 1,2
)
,rn as
(
    select
    rn_pre.Guest, Listing, "Guest ID"
    ,max(case when rn2=1 then rc end) over (partition by Listing) as max_stays_by_single_guest_listing
    ,msd
    from
    rn_pre
    order by 1,2
)
,reference_replace as
(
    select
    "Confirmation Code"
    ,row_number() over (order by "Confirmation Code") as ref_rn
    ,min(Date) as min_date
    from
    reservation_history
    group by 1
)
,pre as
(
    select
    Date as "Payout Date", Type, reference_replace.ref_rn as "Confirmation Code", "Start Date", Nights, rn."Guest ID", rn.max_stays_by_single_guest_listing, a.Listing, Reference, Currency, Amount
    , "Paid Out", "Host Fee" as "Airbnb Cut Host Fee", "Cleaning Fee", "Earnings Year"
    , case when a.details like 'Referral Bonus%' then a.Amount else 0 end as "Referral Amount"
    , date(min("Start Date") over ()) as min_start_date
    , date(max("Start Date") over ()) as max_start_date
    , case when a.Date=reference_replace.min_date then Nights else 0 end as nights_for_occupancy
    , first_value(a."Confirmation Code") over (partition by a.Listing,a.Guest order by a."Start Date")=a."Confirmation Code" as guest_first_listing_visit_bool
    , min(a."Start Date") over (partition by a.Listing,a.Guest) as guest_first_listing_visit_start_date
    , first_value(a."Confirmation Code") over (partition by a.Guest order by a."Start Date")=a."Confirmation Code" as guest_first_visit_bool
    , min(a."Start Date") over (partition by a.Guest) as guest_first_visit_start_date
    , max(a.Date) over () as max_paid_out_date
    from
    reservation_history a
    left join rn
    on a.Guest=rn.Guest
    and a.Listing=rn.Listing
    left join reference_replace
    on a."Confirmation Code"=reference_replace."Confirmation Code"
)
select
a.*
,case
    when a.guest_first_listing_visit_bool
        then FALSE
    else
        coalesce(a."Start Date">a.guest_first_listing_visit_start_date,FALSE)
end as guest_repeats_to_listing_bool
,case
    when a.guest_first_visit_bool
        then FALSE
    else
        coalesce(a."Start Date">a.guest_first_visit_start_date,FALSE)
end as guest_repeats_bool
from
pre a
;
"""

df_export1 = pandas.read_sql_query(query, sqlite_connection)



query="""
with agg as
(
    select
    "Confirmation Code"
    ,Listing
    ,date(min("Start Date")) as "Start Date"
    ,max(Nights) as mn
    ,row_number() over (order by "Confirmation Code") as ref_rn
    from
    reservation_history
    where Listing is not null
    group by 1,2
)
,agg_summarized as
(
    select
    Listing
    from
    agg
    group by 1
)
,dd as
(
    select
    dd."Date"
    ,a.Listing
    from dim_date dd
    left join agg_summarized a
    on true
)
select
a.ref_rn as "Confirmation Code"
,a."Start Date"
,date(a."Start Date",'+'||a.mn||' days') as last_night_of_stay
,dd.Listing
,date(dd.date) as Date
,a.Listing is not null as occupied
,a.mn as "Length of Stay"
from dd
left join agg a
on a.Listing=dd.Listing
and dd.date between a."Start Date" and date(a."Start Date",'+'||a.mn||' days')
order by dd.Date
;
"""

df_export2 = pandas.read_sql_query(query, sqlite_connection)

# create a excel writer object
with pandas.ExcelWriter("airbnb_transactions.xlsx") as writer:
	# use to_excel function and specify the sheet_name and index
	# to store the dataframe in specified sheet
	df_export1.to_excel(writer, sheet_name="airbnb_transactions", index=False)
	df_export2.to_excel(writer, sheet_name="occupancy", index=False)

sqlite_connection.close()
