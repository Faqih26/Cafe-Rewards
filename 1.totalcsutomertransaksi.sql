--total customer melakukan transaksi
-- sebanyak 422 customer belom melakukan transaksi
with b as (
SELECT customer_id,
 sum(case when event = 'transaction' then 1 end ) as total
  FROM [ads].[dbo].[new_data]
  group by customer_id
--order by total 
  )
  -- dari 422 customer terdapat 1881 ads transaksi yang ditawarkan 
select 
	[reward_offer],[offer_type],
	count(*)
from b
inner join [ads].[dbo].[new_data] as nd
on nd.customer_id = b.customer_id
where total is null and event = 'offer received'
group by [reward_offer],[offer_type];

--total customer melakukan transaksi berdasarkan offer type dan reward_oofer yang complete dengan total pembelian 1 
with b as (
SELECT customer_id,
 sum(case when event = 'transaction' then 1 end ) as total
  FROM [ads].[dbo].[new_data]
  group by customer_id
  )
  -- dari 422 customer terdapat 1881 transaksi yang ditawarkan 
select 
	[reward_offer],[offer_type],
	count(*)
from b
inner join [ads].[dbo].[new_data] as nd
on nd.customer_id = b.customer_id
where total = 1 and  event = 'offer completed'
group by [reward_offer],[offer_type]
order by reward_offer;