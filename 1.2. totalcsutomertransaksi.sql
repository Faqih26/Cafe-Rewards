with b as (
SELECT customer_id,
 sum(case when event = 'transaction' then 1 end ) as total,
 	SUM(case when event ='offer received'  and offer_type  != 'informational'  then 1 end) as rec,
	SUM(case when event ='offer completed' then 1 end) as com,
	count(distinct offerid) as offerid_get
	,count( offerid) as offerid_get1
	,CAST( SUM(case when event ='offer completed' then 1 end) as float) 
		/ SUM(case when event ='offer received'  and offer_type  != 'informational'  then 1 end)  as co1m
  FROM [ads].[dbo].[new_data]
  group by customer_id
--order by total 
  )
 , c as( -- dari 422 customer terdapat 1881 ads transaksi yang ditawarkan 
select 
b.*
from b
inner join [ads].[dbo].[new_data] as nd
on nd.customer_id = b.customer_id
where total is not null and event = 'offer received'   and offer_type  != 'informational'
)
select 
 distinct *
--rec,avg(percentages)
from c
--group by rec
  order by rec desc
;
with b as (
SELECT customer_id,
 sum(case when event = 'transaction' then 1 end ) as total,
 	SUM(case when event ='offer received'  and offer_type  != 'informational'  then 1 end) as rec,
	SUM(case when event ='offer completed' then 1 end) as com,
	count(distinct offerid) as offerid_get
	,count( offerid) as offerid_get1
	,CAST( SUM(case when event ='offer completed' then 1 end) as float) 
		/ SUM(case when event ='offer received'  and offer_type  != 'informational'  then 1 end)  as co1m
  FROM [ads].[dbo].[new_data]
  group by customer_id
--order by total 
  )
 , c as( -- dari 422 customer terdapat 1881 ads transaksi yang ditawarkan 
select 
b.*
from b
inner join [ads].[dbo].[new_data] as nd
on nd.customer_id = b.customer_id
where total is not null and event = 'offer received'   and offer_type  != 'informational'
)
select 
--  distinct *
 rec,avg(co1m)
from c
-- where com is not null
 group by rec
  order by rec desc
;


with b as (
SELECT 
	customer_id,
	SUM(case when event ='offer received' and offer_type  != 'informational' then 1 end) as rec,
	SUM(case when event ='offer completed' then 1 end) as com
  FROM [ads].[dbo].[new_data]
  group by customer_id
  )
  , d as (
  select 
  *,
  ROUND( cast(com as float)/rec,2 ) as percentage
  from b

  )
  select
   rec,avg(percentage) as percentages
  --*
  from d
  group by rec
  order by rec desc