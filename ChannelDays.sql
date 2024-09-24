with b as(
SELECT 
	case when event = 'offer received' then time else null end as time_rece,
	*,
    CASE 
        WHEN channels LIKE '%web%' AND channels LIKE '%email%' AND channels LIKE '%mobile%' AND channels LIKE '%social%' THEN 'WEMS'
        WHEN channels LIKE '%web%' AND channels LIKE '%email%' AND channels LIKE '%mobile%' THEN 'WEM'
        WHEN channels LIKE '%web%' AND channels LIKE '%email%' THEN 'WE'
        WHEN channels LIKE '%email%' AND channels LIKE '%mobile%' AND channels LIKE '%social%' THEN 'EMS'
        ELSE NULL
    END AS kategori_channels
FROM [ads].[dbo].[new_data]
where event != 'transaction'
)

select
 
*
from b
  --where customer_id= '018a49ffb8cf4812903e7c1f56fbb0b0'
  order by customer_id,time