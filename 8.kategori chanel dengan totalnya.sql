with b as(
SELECT 
	*,
    CASE 
        WHEN channels LIKE '%web%' AND channels LIKE '%email%' AND channels LIKE '%mobile%' AND channels LIKE '%social%' THEN 'WEMS'
        WHEN channels LIKE '%web%' AND channels LIKE '%email%' AND channels LIKE '%mobile%' THEN 'WEM'
        WHEN channels LIKE '%web%' AND channels LIKE '%email%' THEN 'WE'
        WHEN channels LIKE '%email%' AND channels LIKE '%mobile%' AND channels LIKE '%social%' THEN 'EMS'
        ELSE NULL
    END AS kategori_channels
FROM [ads].[dbo].[new_data]
)
select
	kategori_channels,
	event,
	count(*) as total,
	max(count(*) ) over(partition by kategori_channels) as max_total
from b
group by kategori_channels,event
order by kategori_channels