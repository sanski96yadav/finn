with next_request_form as 
(
   Select
      mapped_user_id as user_id,
      timestamp as current_request,
      lead(timestamp) over (partition by mapped_user_id 
   order by
      timestamp) as next_request-------to implement 10 mins logic
   from
      `finn_interview.requests_log` 
)
,
time_between_sequential_request as 
(
   Select
      *,
      timestamp_diff(next_request, current_request, millisecond) as interval_millisec------millisecond chosen as timestamp column has millisecond granularity
   from
      next_request_form 
)
,
request_exceeding_ten_mins as 
(
   Select
      *,
      case
         when
            next_request is null 
            or interval_millisec > 600000--------converted 10 mins to millisecond
         then
            1 
         else
            0 
      end
      as new_application 
   from
      time_between_sequential_request 
)
, new_application_user_level as 
(
   Select
      *,
      sum(new_application)over (partition by user_id 
   order by
      current_request) as sum_new_application,
      extract(week 
   from
      current_request) as calendar_week,
      date_trunc(date(current_request), week) as firstday_week 
   from
      request_exceeding_ten_mins 
)
,
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
)
,
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
)
,
extract_roles as 
(
   SELECT
      user_id,
      coalesce(regexp_extract(roles, '[a-z]+'), 
      (
         regexp_extract(roles, '[a-z]+","[a-z]+') 
      )
) as roles 
   from
      `finn_interview.dim_user_roles` 
)
,
join_roles as 
(
   SELECT
      roles,
      count(*) as application_submitted 
   from
      merge_grouped_new_application a 
      left join
         extract_roles r 
         on a.user_id = r.user_id 
   group by
      1 
)
Select
   case
      when
         roles is null 
      then
         'unknown' 
      else
         roles 
   end
   as roles,
application_submitted 
from
   join_roles 
order by
   application_submitted desc
