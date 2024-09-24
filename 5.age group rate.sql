with b as (
SELECt *
	  ,cast(amount as float) as amounts
	  ,cast([reward_get] as float) as reward_gets
	  ,case 
			when age <= 24 then 'Young'
			when age <= 44 then 'Adult'
			when age <= 64 then 'Middle-Aged'
			else 'Senior'
		end as klasifikasi_age

  FROM [ads].[dbo].[new_data]
  )
 ,c as
 (
select 
	klasifikasi_age,gender,
	 sum (case when event = 'offer received' then 1 end) as offer_received, 
	 sum (case when event = 'offer viewed' then 1 end) as offer_viewed, 
	 sum (case when event = 'offer completed' then 1 end) as offer_complete,
	 sum( amounts ) as total_amount,
	 sum( reward_gets  ) as Reward_Give
from b
group by klasifikasi_age,gender

	)
SELECT 
    klasifikasi_age,gender,
   -- offer_received,
	--SUM(offer_received) OVER() AS of_rece,
	offer_received/CAST( SUM(offer_received) OVER() as float)*100 as of_rece_percentage,
   -- offer_viewed,
	--SUM(offer_viewed) OVER() AS of_vie,
	offer_viewed/CAST( SUM(offer_viewed) OVER() as float)*100 as of_view_percentage,
   -- offer_complete,
	--SUM(offer_complete) OVER() AS of_co,
	offer_complete/CAST( SUM(offer_complete) OVER() as float)*100 as of_comp_percentage,
    total_amount,
    Reward_Give
FROM c
order by klasifikasi_age