with b as (
	SELECT 
		distinct customer_id,
		offer_type
	  FROM [ads].[dbo].[new_data]
	  where offer_type = 'informational' -- sebanyak 10547 customer mendapatkan offer informational 
	  )
,c as (
SELECT 
		 
		distinct customer_id,offer_type
	  FROM [ads].[dbo].[new_data]
	  where offer = 'Transaction With Offer'
	  )
	  -- dari 10547 customer yang mendapatkan offer informational sebanyak 8301 melakukan transaksi with offer dengan dan 2246 tidak melakukan transaksi dengan offer
	  -- mari cari tahu dari 8301 ofer informational yang diberikan pada customer offer apa yang paling banyak menyebabkan customer melakukan transaksi
, f as (
select * from b 
where  exists (select 1 from c where b.customer_id = c.customer_id)
)-- terdapat 11763 rows atau informational yang diberikan pada 8301 customer dimana dari kedua ofer yang diberikan offer dengan 8bed yang paling banyak diterima
 -- (setiap customer bisa mendapatkan bbrp offer informational lebih dari 1 kali dengan offer informational yang sama hal ini yang menyebabkan offer informational lebih dari 10547)
select 
-- f.customer_id,
nd.OfferId,
--nd.time,nd.event
count(f.customer_id) as total
from f
inner join [ads].[dbo].[new_data] as nd
on f.customer_id = nd.customer_id
where nd.offer_type = 'informational' and nd.event = 'offer received'
group by nd.OfferId 
--order by nd.customer_id 