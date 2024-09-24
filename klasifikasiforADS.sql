--use ads;
--CREATE OR ALTER VIEW [dbo].klasifikasiforADS AS
with b as (
	select 
	*,
	case 
			when age <= 34 then 'Muda'
			when age <= 54 then 'Dewasa'
			when age <= 74 then 'Paruh Baya'
			else 'Lanjut Usia'
		end as klasifikasi_age
	FROM [ads].[dbo].[new_data]
)
, c as (
SELECT 
	[OfferId],
	gender,
	klasifikasi_age,
	o.[offer_type],
    o.[difficulty],
	dense_rank() over(partition by o.[offer_type] order by o.[difficulty] ) as rn_diff,
    o.[reward],
	rank() over(partition by gender,klasifikasi_age order by count([OfferId]) desc ) as rn ,
	count([OfferId]) as total,
	avg( cast( amount as float)) as avg_amount_spend,
	sum( cast( amount as float)) as sum_amount_spend,
	min(cast( amount as float)) as min_amount_spend,
	max(cast( amount as float)) as max_amount_spend
  FROM b
  inner join [ads].[dbo].[offers] as o
  on o.[offer_id] = b.[OfferId]
  where offer = 'Transaction With Offer'
  group by [OfferId],gender,klasifikasi_age,o.[offer_type],o.[difficulty],o.[reward]
 -- order by o.[offer_type],[new_offer_id],rn,rn_diff 
  )
  select 
  *
  from c
  --order by [offer_type],[OfferId],avg_amount_spend,rn,rn_diff 