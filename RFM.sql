--use ads;
--CREATE OR ALTER VIEW [dbo].RFM AS
with clean_re as (
SELECT 
	customer_id,
	[time],
	coalesce(
				lead([time]) over(partition by customer_id  order by [time] ) ,
				MAX([time]) over(order by [time] desc ) ) as next_order_date ,
		DATEDIFF(day,[time],
			coalesce
					(
						lead([time]) over(partition by customer_id  order by [time] ) ,
						MAX([time]) over(order by [time] desc ) 
					)
					) as time_diff
		,case 
			when age <= 24 then 'Young'
			when age <= 44 then 'Adult'
			when age <= 64 then 'Middle-Aged'
			else 'Senior'
		end as klasifikasi_age
	--,max(time) over(partition by customer_id order by time desc) as maxTime
  FROM [ads].[dbo].[new_data]
where event = 'transaction'
--order by customer_id , time
)
, Recency as
(
	select 
		customer_id,
		klasifikasi_age,
		avg(time_diff) as avg_day_diff,
		NTILE(5) OVER (order by avg(time_diff) desc) as  rfm_recency
	from clean_re
	group by customer_id,klasifikasi_age
)
,Frequency as (
select
	[customer_id],
	count( [event]) as count_order,
	NTILE(5) OVER (order by count([event])) as  rfm_frequency
from [ads].[dbo].[new_data]
where [event] = 'transaction'
group by [customer_id]
)
,monetary as (
select 
	[customer_id],
	gender,
	min(tahun) as tahun,
	sum(cast([Amount] as float)) as monetary,
	avg(cast([Amount] as float)) as avg_monetary,
	sum(case when [offer] = 'Transaction With Offer' then 1 end ) as  transaksi_withOffer,
	sum(case when [offer] = 'Transaction Without Offer' then 1 end ) as  transaksi_witOuthOffer,
	sum(case when event = 'offer received' and offer_type != 'informational' then 1 end ) as Get_Offer,
	NTILE(5) OVER (order by sum(cast( [Amount] as float))) as  rfm_monetary
from [ads].[dbo].[new_data]
--where [event] = 'transaction'
group by [customer_id],gender
)
, RFM as  (
select 
	r.[customer_id],
	r.klasifikasi_age,
	m.tahun,
	rfm_recency as recency ,
	rfm_frequency as frequency,
	rfm_monetary as monetary ,
	r.avg_day_diff,
	f.count_order,
	m.monetary as total_money,
	m.avg_monetary as avg_totalMoney,
	m.transaksi_withOffer,
	m.transaksi_witOuthOffer,
	m.Get_Offer,
	m.gender,
	concat( (rfm_recency ),'-',(rfm_frequency ),'-', (rfm_monetary))rfm_cell_string

from Recency as r
inner join  frequency as f on r.[customer_id] = f.[customer_id]
inner join  monetary as m  on m.[customer_id] = f.[customer_id]
)
select 
*,
case 
  WHEN rfm_cell_string IN ('5-5-5', '5-5-4', '5-4-4', '5-4-5', '4-5-4', '4-5-5', '4-4-5') THEN 'Champion'
  WHEN rfm_cell_string IN ('5-4-3', '4-4-4', '4-3-5', '3-5-5', '3-5-4', '3-4-5', '3-4-4', '3-3-5') THEN 'Loyal'
  WHEN rfm_cell_string IN ('5-5-3', '5-5-1', '5-5-2', '5-4-1', '5-4-2', '5-3-3', '5-3-2', '5-3-1', 
       '4-5-2', '4-5-1', '4-4-2', '4-4-1', '4-3-1', '4-5-3', '4-3-3', '4-3-2', 
       '4-2-3', '3-5-3', '3-5-2', '3-5-1', '3-4-2', '3-4-1', '3-3-3', '3-2-3') THEN 'Potential Loyalist'
  WHEN rfm_cell_string IN ('5-1-2', '5-1-1', '4-2-2', '4-2-1', '4-1-2', '4-1-1', '3-1-1') THEN 'New Costumer'
  WHEN rfm_cell_string IN ('5-2-5', '5-2-4', '5-2-3', '5-2-2', '5-2-1', '5-1-5', '5-1-4', '5-1-3', 
       '4-2-5', '4-2-4', '4-1-3', '4-1-4', '4-1-5', '3-1-5', '3-1-4', '3-1-3') THEN 'Promising'
  WHEN rfm_cell_string IN ('5-3-5', '5-3-4', '4-4-3', '4-3-4', '3-4-3', '3-3-4', '3-2-5', '3-2-4') THEN 'Needs Attention'
  WHEN rfm_cell_string IN ('3-3-1', '3-2-1', '3-1-2', '2-2-1', '2-1-3', '2-3-1', '2-4-1', '2-5-1') THEN 'About To Sleep'
  WHEN rfm_cell_string IN ('2-5-5', '2-5-4', '2-4-5', '2-4-4', '2-5-3', '2-5-2', '2-4-3', '2-4-2', 
       '2-3-5', '2-3-4', '2-2-5', '2-2-4', '1-5-3', '1-5-2', '1-4-5', '1-4-3', 
       '1-4-2', '1-3-5', '1-3-4', '1-3-3', '1-2-5', '1-2-4') THEN 'At Risk'
  WHEN rfm_cell_string IN ('1-5-5', '1-5-4', '1-4-4', '2-1-4', '2-1-5', '1-1-5', '1-1-4', '1-1-3') THEN 'Cannot Lose Them'
  WHEN rfm_cell_string IN ('3-3-2', '3-2-2', '2-3-3', '2-3-2', '2-2-3', '2-2-2', '1-3-2', '1-2-3', 
       '1-2-2', '2-1-2', '2-1-1') THEN 'Hibernating Costumer'
  WHEN rfm_cell_string IN ('1-1-1', '1-1-2', '1-2-1', '1-3-1', '1-4-1', '1-5-1') THEN 'Lost Costumer'
 end rfm_segment

from RFM
--order by customer_id