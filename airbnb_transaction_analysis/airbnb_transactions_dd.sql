select
cast('Type' as varchar(500)) as "Field Name"
,cast('dimension' as varchar(500)) as "Field Type"
,cast('Type of transaction e.g. reservation, resolution adjustment' as varchar(500)) as Description
,'Text' as "Data Type"

union all

select
cast('Start Date' as varchar(500)) as "Field Name"
,'dimension' as "Field Type"
,cast('Start date of stay' as varchar(500)) as Description
,'Date' as "Data Type"

union all

select
cast('Listing' as varchar(500)) as "Field Name"
,'dimension' as "Field Type"
,cast('Listing Name' as varchar(500)) as Description
,'Text' as "Data Type"

union all

select
cast('Currency' as varchar(500)) as "Field Name"
,'dimension' as "Field Type"
,cast('Currency of payout' as varchar(500)) as Description
,'Text' as "Data Type"

union all

select
cast('occupied' as varchar(500)) as "Field Name"
,'dimension' as "Field Type"
,cast('True if the listing was occupied that date (Listing Name is not null); else false' as varchar(500)) as Description
,'Boolean' as "Data Type"

union all

select
cast('Length of Stay' as varchar(500)) as "Field Name"
,'measure' as "Field Type"
,cast('Length of stay (nights)' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Cleaning Fee' as varchar(500)) as "Field Name"
,'measure' as "Field Type"
,cast('The cleaning fee charged to the guest' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Max Stays By Single Guest Listing' as varchar(500)) as "Field Name"
,'measure' as "Field Type"
,cast('For a listing, the most stays a single has recorded' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Airbnb Cut Host Fee' as varchar(500)) as "Field Name"
,'measure' as "Field Type"
,cast('The that Airbnb takes from hosts - around 3% as of writing' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Min Start Date' as varchar(500)) as "Field Name"
,'dimension' as "Field Type"
,cast('The minimum start date in the entire dataset' as varchar(500)) as Description
,'Date' as "Data Type"

union all

select
cast('Max Start Date' as varchar(500)) as "Field Name"
,'dimension' as "Field Type"
,cast('The maximum start date in the entire dataset' as varchar(500)) as Description
,'Date' as "Data Type"

union all

select
cast('Referral Amount' as varchar(500)) as "Field Name"
,'measure' as "Field Type"
,cast('The amount Airbnb paid out for a host referral' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Occupancy' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('Calculated at the [Listing] level: Days Occupied / Count of Days between parameters [Occupancy Start Date] and [Occupancy End Date]' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Avg Take Home/Night' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('[Take Home (After Fees)]/sum([nights_for_occupancy])' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Total Host Payout' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('sum([Amount]); note this includes the cleaning fee' as varchar(500)) as Description
,'Numeric' as "Data Type"


union all

select
cast('Take Home (After Fees)' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('sum([Amount]) - sum([Cleaning Fees]); this is called take home pay under the assumption that the cleaning fee goes directly to a cleaner. The field [Amount] already excludes the Airbnb Cut Host Fee' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Distinct Stays' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('Count of distinct stays with confirmation code uniquely identifying stays' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Distinct Guests' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('Count of distinct [Guest ID]' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Max Length of Stay' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('Max([Length of Stay])' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Distinct Listing' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('count of distinct [Listing]' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Take Home %' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('[Take Home (After Fees)]/sum([Amount])' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('nights_for_occupancy' as varchar(500)) as "Field Name"
,'Measure' as "Field Type"
,cast('[Length of Stay] but populated only once per [Confirmation Code] to avoid duplicate counting' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Nights Occupied' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('sum([nights_for_occupancy])' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Host Referral Bonuses' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('sum([Referral Amount])' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Date Filter' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('TRUE if [Date]>= parameter [Occupancy Start Date] and [Date]>=[Occupancy End Date]; else FALSE.' as varchar(500)) as Description
,'Boolean' as "Data Type"

union all

select
cast('Airbnb Cut Host Fee %' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('sum([Airbnb Cut Host Fee])/[Total Host Payout]' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Cleaning Fee %' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('sum([Cleaning Fee])/[Total Host Payout]' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('New Guests' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('count distinct [Guest ID] if the visit is the first visit for that [Guest ID]' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Repeat Business %' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('[Repeat Stays]/[Distinct Stays]' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Repeat Stays' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('count distinct [Confirmation Code] if the visit is the 2nd+ visit for that [Guest ID]' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Confirmation Code' as varchar(500)) as "Field Name"
,cast('dimension' as varchar(500)) as "Field Type"
,cast('Numeric identifier unique to a stay - this value is generated in transformation and not an Airbnb confirmation code' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('last_night_of_stay' as varchar(500)) as "Field Name"
,'dimension' as "Field Type"
,cast('Ending date of stay' as varchar(500)) as Description
,'Date' as "Data Type"

union all

select
cast('Nights Available' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('Count of nights available' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Check Out Date' as varchar(500)) as "Field Name"
,'dimension' as "Field Type"
,cast('Check Out Date' as varchar(500)) as Description
,'Date' as "Data Type";