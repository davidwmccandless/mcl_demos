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