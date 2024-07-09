with next_request_form as 
(
Select
mapped_user_id as user_id,
timestamp as current_request,
lead(timestamp) over (partition by mapped_user_id order by timestamp) as next_request -------to implement 10 mins logic
from
`finn_interview.requests_log` 
),
time_between_sequential_request as 
(
Select
*,
timestamp_diff(next_request, current_request, millisecond) as interval_millisec------millisecond chosen as timestamp column has millisecond granularity
from
next_request_form
),
request_exceeding_ten_mins as 
(
Select
*,
case
when
next_request is null 
or interval_millisec > 600000 --------converted 10 mins to millisecond
then
1
else
0
end
as new_application 
from
time_between_sequential_request
), 
new_application_user_level as 
(
Select
*,
sum(new_application)over (partition by user_id order by current_request) as sum_new_application,
extract(week from current_request) as calendar_week,
date_trunc(date(current_request),week) as firstday_week 
from
request_exceeding_ten_mins
),
grouped_request_application_table as 
(
Select
user_id,
calendar_week,
firstday_week,
min(current_request) as current_request,
max(next_request) as next_request,
'same_application' as application_type 
from
new_application_user_level
where
new_application = 0 
group by
user_id,
sum_new_application,
calendar_week,
firstday_week 
),
new_application_table as 
(
SELECT
user_id,
calendar_week,
firstday_week, 
current_request,
next_request,
'new_application' as application_type 
from
new_application_user_level
where
new_application = 1 
),
merge_grouped_new_application as 
(
Select
* 
from
grouped_request_application_table
union all
select
* 
from
new_application_table
),
number_of_days_week as 
(
Select
*,
count(distinct date(current_request))over (partition by firstday_week) as days_in_week 
from
merge_grouped_new_application 
)
Select
count(*) as applications_total,
count(distinct calendar_week) as no_of_weeks,
round((count(*) / count(distinct calendar_week)),0)  as average_per_week 
from
number_of_days_week
where
days_in_week = 7
