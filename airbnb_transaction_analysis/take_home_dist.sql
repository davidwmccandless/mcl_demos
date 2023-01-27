with agg as
(
    select
    Listing
    ,"Confirmation Code"
    ,coalesce(sum(Amount),0)-coalesce(min("Cleaning Fee"),0) as "Sum Take Home"
    ,min(Nights) as mn
    ,min("Start Date") as "Start Date"
    from
    reservation_history
    where Listing is not null
    group by 1,2
)
select
Listing
, "Sum Take Home"/mn as "Avg Take Home / Night ($)"
, "Sum Take Home"
, "Start Date"
from
agg;