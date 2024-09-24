-- berapa lama customer yang mendapatkan offer akan melakukan transaksi dengan without offer
--- (next cari berapa laam customer melakukan transaksi dari transaksi sebelumnya) sudah no 2 
with b as (
SELECT [OfferId]
      ,[customer_id]
      ,[event]
      ,[time]
	  ,lead([OfferId]) over(partition by customer_id order by time) as idsa
	  ,lead([time]) over(partition by customer_id order by time) as timesa
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
      ,[time]
	  ,  idsa
	  , timesa
	  ,timesa-time as hour_nextBuy
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
  from b 
  where [OfferId] is not null and idsa is null
   -- order by [customer_id] , time -- ada bbrp transaksi yang menggunakan offer tapi belum melakukan transaksi lagi meneyebabkan timesa menjadi null
	)-- rata rata 50 jam akan melakukan transaksi lagi setelah mendapatkan transaksi dengan offer discount atau bogo
select 
[OfferId],
[offer_type],
AVG(hour_nextBuy) as next_hourbuy
from c 
group by [OfferId],[offer_type]
order by [offer_type] , next_hourbuy asc