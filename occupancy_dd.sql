

select
cast('Confirmation Code' as varchar(500)) as "Field Name"
,cast('dimension' as varchar(500)) as "Field Type"
,cast('Numeric identifier unique to a stay - this value is generated in transformation and not an Airbnb confirmation code' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Start Date' as varchar(500)) as "Field Name"
,'dimension' as "Field Type"
,cast('Start date of stay' as varchar(500)) as Description
,'Date' as "Data Type"


union all

select
cast('last_night_of_stay' as varchar(500)) as "Field Name"
,'dimension' as "Field Type"
,cast('Ending date of stay' as varchar(500)) as Description
,'Date' as "Data Type"

union all

select
cast('Listing' as varchar(500)) as "Field Name"
,'dimension' as "Field Type"
,cast('Listing Name' as varchar(500)) as Description
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
,cast('Length of stay' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Occupancy' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('Days Occupied / Count of Days between parameters [Occupancy Start Date] and [Occupancy End Date]' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Distinct Stays' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('Count of distinct stays with confirmation code uniquely identifying stays' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Nights Occupied' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('Count of nights occupied' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Nights Available' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('Count of nights available' as varchar(500)) as Description
,'Numeric' as "Data Type"

union all

select
cast('Date Filter' as varchar(500)) as "Field Name"
,'Calculated Measure' as "Field Type"
,cast('TRUE if Date between parameters [Occupancy Start Date] and [Occupancy End Date]; else FALSE' as varchar(500)) as Description
,'Boolean' as "Data Type"
;

