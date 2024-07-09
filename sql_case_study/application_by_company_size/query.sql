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
or interval_millisec > 600000 --------converted 10 mins to millisecond (10*60*1000)
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
)
,
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
count_cars as 
(
SELECT
user_id,
company_id,
sum(company_fleet_size) over(partition by company_id) as total_cars 
FROM
`finn-428616.finn_interview.user_company` 
order by
company_id 
),
company_size_on_cars as
(
Select
*,
case
when
total_cars <= 50 
then
'Small' 
when
total_cars <= 150 
then
'Medium' 
else
'Large' 
end
as company_size 
from
count_cars
)
Select 
distinct company_size,
count(*) as applications_total,
from merge_grouped_new_application a
left join company_size_on_cars c on a.user_id=c.user_id
group by 1
order by applications_total desc
