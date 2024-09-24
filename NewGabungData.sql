--use ads;
--CREATE OR ALTER VIEW [dbo].new_data AS
with New_Datab as (
SELECT
	  -- Event
	   e.[customer_id]
      ,e.[event]
      ,e.[time]
      ,e.[Amount]
      ,e.[OfferId]
      ,e.[Reward]
	  -- Offers
      ,o.[offer_type]
      ,o.[difficulty]
      ,o.[duration]
      ,o.[channels]
	  -- Customers
      ,c.[Tahun]
      ,c.[Bulan]
      ,c.[Tanggal]
      ,c.[became_member_on]
      ,c.[gender]
      ,c.[age]
      ,c.[income]
  FROM [ads].[dbo].[events] as e
 left join [ads].[dbo].[offers] as o
 on e.OfferId = o.offer_id
 left join
	[ads].[dbo].[customers_new] as c
	on c.customer_id = e.customer_id
)
, data_tra as 
(   SELECT
	   lead(case when event = 'offer completed' then [OfferId] end)  OVER (PARTITION BY customer_id,[time] ORDER BY [time])  AS le -- sesudah
      ,LAG(case when event = 'offer completed' then [OfferId] end) OVER (PARTITION BY customer_id,[time] ORDER BY [time])AS la --sebelum
	  ,LEAD(case when event = 'offer completed' then [offer_type] end) OVER (PARTITION BY customer_id,[time] ORDER BY [time])  AS le_of
      ,LAG(case when event = 'offer completed' then [offer_type] end) OVER (PARTITION BY customer_id,[time] ORDER BY [time]) AS la_of
      ,[customer_id]
      ,[event]
      ,[time]
      ,[Amount]
      ,[OfferId]
      ,[Reward]
      ,[offer_type]
      ,[difficulty]
      ,[duration]
      ,[channels]
      ,[Tahun]
      ,[Bulan]
      ,[Tanggal]
      ,[became_member_on]
      ,[gender]
      ,[age]
      ,[income]
    FROM New_Datab
	where offer_type != 'informational' or offer_type is null
)
, GabungAllData as (
SELECT
		[customer_id],
		[event],
		[time],
		[Amount],
		COALESCE([OfferId],le, la  ) AS [OfferId],
		[Reward] as reward_get,
		[Tahun],
		[Bulan],
		[Tanggal],
		became_member_on,
		gender,
		age,
		income,
		COALESCE(offer_type,le_of, la_of  ) offer_type,
		difficulty,
		reward AS reward_offer,
		duration,
		channels
from data_tra
union all
   SELECT
        [customer_id],
        [event],
        [time],
        [Amount],
        [OfferId],
        [Reward] AS reward_get,
		[Tahun],
		[Bulan],
		[Tanggal],
        became_member_on,
        gender,
        age,
        income,
        offer_type,
        difficulty,
        reward AS reward_offer,
        duration,
        channels

    FROM New_Datab
where  offer_type = 'informational'
), klasfikasiTransaksi as (
select
	case
		when 
			offer_type = 'informational' then 'offer'
		when 
			OfferId is not null and event ='transaction' then 'Transaction With Offer' 
		when 
			OfferId is null and event ='transaction' then 'Transaction Without Offer' 
		else 'Offer' 
		end 
		as offer,
		*
from GabungAllData
)
select 
distinct	* 
from klasfikasiTransaksi
