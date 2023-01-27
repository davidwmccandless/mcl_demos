with agg as
(
    select
    Listing
    ,Guest
    ,count(distinct "Confirmation Code") as "Visits Per Guest"
    from
    reservation_history
    where listing is not null
    group by 1,2
)
select
Listing
,"Visits Per Guest"
from
agg;