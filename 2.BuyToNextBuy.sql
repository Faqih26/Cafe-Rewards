--  cari berapa laam customer melakukan transaksi dari transaksi sebelumnya ke selanjutnya
with b as (
SELECT [OfferId]
      ,[customer_id]
      ,[event]
      ,[time]
	  ,lead([OfferId]) over(partition by customer_id order by time) as idsa
	  ,lead([time]) over(partition by customer_id order by time) as timesa
	  ,[offer_type]
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
      ,[time]
	  ,  idsa
	  , timesa
	  ,timesa-time as hour_nextBuy
	  ,[offer_type]
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
      ,[difficulty]
      ,[reward_offer]
      ,[duration]
      ,[channels]
  from b 
	)-- rata rata 66 jam akan melakukan transaksi lagi setelah mendapatkan transaksi dengan offer discount atau bogo
select 

AVG(hour_nextBuy) as next_hourbuy
from c 