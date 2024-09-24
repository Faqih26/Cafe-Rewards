-- setelah melakukan transaksi dengam offer apakah transaksi selanjutnya lebih besar apa kecil ?
with b as (
SELECT [OfferId]
      ,[customer_id]
      ,[event]
      ,[time]
	  ,lead([OfferId]) over(partition by customer_id order by time) as idsa
	  ,lead([time]) over(partition by customer_id order by time) as timesa
	  ,lead([Amount]) over(partition by customer_id order by time) as amount_nextbuy
      ,[Amount]
      ,[offer]
      ,[reward_get]
      ,[Tahun]
      ,[Bulan]
      ,[Tanggal]
      ,[became_member_on]
      ,[gender]
      ,[age]
      ,[income]
      ,[offer_type]
      ,[difficulty]
      ,[reward_offer]
      ,[duration]
      ,[channels]
  FROM [ads].[dbo].[new_data]
  where [offer] = 'Transaction With Offer' or offer = 'Transaction Without Offer'

  ) , c as (
  select [OfferId]
      ,[customer_id]
      ,[event]
	  ,  idsa
	   ,[time]
	  , timesa
	  ,timesa-time as hour_nextBuy
      ,[Amount]
	  ,amount_nextbuy
	  , case when [Amount] > amount_nextbuy then 'amount meningkat' else 'amount menurun' end as kat 
      ,[offer]
      ,[reward_get]
      ,[Tahun]
      ,[Bulan]
      ,[Tanggal]
      ,[became_member_on]
      ,[gender]
      ,[age]
      ,[income]
      ,[offer_type]
      ,[difficulty]
      ,[reward_offer]
      ,[duration]
      ,[channels]
  from b 
  where [OfferId] is not null and idsa is null
   -- order by [customer_id] , time -- ada bbrp transaksi yang menggunakan offer tapi belum melakukan transaksi lagi meneyebabkan timesa menjadi null
	)
select 

OfferId,kat,count(*) as total_offer,SUM(cast (amount as float) )as total_amount
from c
group by OfferId,kat
--order by OfferId,kat