with rate as (
	select phrase
		 , toHour(dt) as hours
		 , max(views) - min(views) as views
	from test.phrases_views
	where dt > toDateTime('2025-01-01 00:00:00') and campaign_id = 1111111
	group by phrase, hours
	having views <> 0
)
select phrase, groupArray(tuple(hours, views)) as views_by_hour
from rate
group by phrase