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
    Date as "Payout Date", Type, reference_replace.ref_rn as "Confirmation Code", "Start Date", Nights as "Length of Stay", rn."Guest ID", rn.max_stays_by_single_guest_listing, a.Listing, Currency, Amount
    , "Paid Out", "Host Fee" as "Airbnb Cut Host Fee", "Cleaning Fee", "Earnings Year"
    , case when a.details like 'Referral Bonus%' then a.Amount else 0 end as "Referral Amount"
    , date(min("Start Date") over ()) as min_start_date
    , date(max("Start Date") over ()) as max_start_date
    , case when a.Date=reference_replace.min_date then Nights else 0 end as nights_for_occupancy
    , first_value(a."Confirmation Code") over (partition by a.Listing,a.Guest order by a."Start Date")=a."Confirmation Code" as guest_first_listing_visit_bool
    , min(a."Start Date") over (partition by a.Listing,a.Guest) as guest_first_listing_visit_start_date
    , first_value(a."Confirmation Code") over (partition by a.Guest order by a."Start Date")=a."Confirmation Code" as guest_first_visit_bool
    , min(a."Start Date") over (partition by a.Guest) as guest_first_visit_start_date
    , max(a.Date) over () as max_pay_out_date
    , a."index"
    from
    reservation_history a
    left join rn
    on a.Guest=rn.Guest
    and a.Listing=rn.Listing
    left join reference_replace
    on a."Confirmation Code"=reference_replace."Confirmation Code"
)
,agg as
(
    select
    "Confirmation Code"
    ,Listing
    ,date(min("Start Date")) as "Start Date"
    ,max("Length of Stay") as mn
    from
    pre
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
,pen as
(
    select
    a."Confirmation Code"
    ,a."Start Date"
    ,date(a."Start Date",'+'||a.mn||' days') as last_night_of_stay
    ,date(a."Start Date",'+'||(a.mn+1)||' days') as checkout_date
    ,dd.Listing
    ,date(dd.date) as Date
    ,a.Listing is not null as occupied
    ,a.mn as "Length of Stay"
    ,max(date(a."Start Date",'+'||a.mn||' days')) over () as max_last_night_of_stay
    ,row_number() over (partition by a."Confirmation Code") as rn
    from dd
    left join agg a
    on a.Listing=dd.Listing
    and dd.date between a."Start Date" and date(a."Start Date",'+'||a.mn||' days')
    order by dd.Date
)
,checkout_dates as
(
    select
    listing
    ,checkout_date
    from
    pen
    where listing is not null and checkout_date is not null
    group by 1,2
)
select
pen."Confirmation Code"
,pen."Start Date"
,pen.last_night_of_stay
,pen.Listing
,pen.Date
,pen.occupied
,pen."Length of Stay"
,pen.max_last_night_of_stay
,pre."Payout Date"
,pre.Type
,pre."Guest ID"
,pre.max_stays_by_single_guest_listing
,pre.Currency
,pre.Amount
,pre."Paid Out"
,pre."Airbnb Cut Host Fee"
,pre."Cleaning Fee"
,pre."Earnings Year"
,pre.min_start_date
,pre.max_start_date
,pre.nights_for_occupancy
,pre.guest_first_listing_visit_bool
,pre.guest_first_listing_visit_start_date
,pre.guest_first_visit_bool
,pre.guest_first_visit_start_date
,pre.max_pay_out_date
,case
    when pre.guest_first_listing_visit_bool
        then FALSE
    else
        coalesce(pre."Start Date">pre.guest_first_listing_visit_start_date,FALSE)
end as guest_repeats_to_listing_bool
,case
    when pre.guest_first_visit_bool
        then FALSE
    else
        coalesce(pre."Start Date">pre.guest_first_visit_start_date,FALSE)
end as guest_repeats_bool
,pre."Referral Amount"
,cd.Listing is not null as checkout_bool
from
pen
left join pre
on pen.rn=1
and
pen."Confirmation Code"=pre."Confirmation Code"
left join checkout_dates cd
on pen.Listing=cd.Listing
and pen.Date=cd.checkout_date
where date<=max_last_night_of_stay

union all

-- now get referrals
select
null as "Confirmation Code"
,null as "Start Date"
,null as last_night_of_stay
,null as Listing
,pen.Date
,null as occupied
,null as "Length of Stay"
,max_last_night_of_stay
,pre."Payout Date"
,pre.Type
,null as "Guest ID"
,null as max_stays_by_single_guest_listing
,pre.Currency
,pre.Amount
,pre."Paid Out"
,0 as "Airbnb Cut Host Fee"
,0 as "Cleaning Fee"
,pre."Earnings Year"
,null as min_start_date
,null as max_start_date
,0 as nights_for_occupancy
,null as guest_first_listing_visit_bool
,null as guest_first_listing_visit_start_date
,null as guest_first_visit_bool
,null as guest_first_visit_start_date
,pre.max_pay_out_date
,null as guest_repeats_to_listing_bool
,null as guest_repeats_bool
,pre."Referral Amount"
,null as checkout_bool
from
pen
inner join pre
on pen.rn=1
and pre."Referral Amount">0
and pen."Date"=date(pre."Payout Date")
order by date
;
