SELECT [OfferId]
      ,[event]
	  ,offer_type
	  ,count(event) as total
	  ,max(count(event)) over(partition by [OfferId]) as max_total
	  ,round( cast( count(event) as float)
				/max(count(event)) over(partition by [OfferId]),2)  as percentages
  FROM [ads].[dbo].[new_data]
  group by [OfferId],[event],offer_type
  order by [OfferId]