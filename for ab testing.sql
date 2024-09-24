--use ads;
--CREATE OR ALTER VIEW [dbo].ADS_Transaction_OfferReceived AS -- untuk melihat kupon expired
WITH ranked_offers AS (
    SELECT 
        [offer],
        [customer_id],
        [event],
        [time],
        [Amount],
        [OfferId],
        [offer_type],
        [duration],
        ROW_NUMBER() OVER (PARTITION BY [customer_id], [OfferId] ORDER BY [time]) AS offer_sequence
    FROM [ads].[dbo].[new_data]
    WHERE [event] = 'offer received'
)
, cc as (
SELECT 
    -- Generate new ID only for 'offer received' events
    CASE 
        WHEN nd.[event] = 'offer received' 
        THEN CONCAT(nd.[OfferId], '_', ro.offer_sequence) 
        ELSE nd.[OfferId] 
    END AS final_offer_id
	,nd.*
FROM [ads].[dbo].[new_data] nd
LEFT JOIN ranked_offers ro
    ON nd.[customer_id] = ro.[customer_id] 
    AND nd.[OfferId] = ro.[OfferId]
    AND nd.[time] = ro.[time]
	)
,r as  ( -- filter data hanya untuk yang event = 'offer completed' or offer = 'Transaction With Offer' or offer_type = 'informational'
    SELECT 
		CONCAT(customer_id, '_', OfferId, '_', CAST(time AS VARCHAR)) AS new_id,
        OfferId,
        customer_id
    FROM 
        cc
    WHERE 
--        customer_id = '00cf471ed1aa42a8bdde5561d67da2b1' AND
         event = 'offer completed' or offer = 'Transaction With Offer' or offer_type = 'informational'
)
,c as ( -- mengambil semua data yang ada lalu buat time transaction
SELECT 
	CONCAT(customer_id, '_', OfferId, '_', CAST(time AS VARCHAR)) AS new_id,
	case when event = 'transaction' then time else null end as time_transaction,
    nd.*
FROM 
    cc AS nd
	where event != 'offer viewed' 
--where event != 'offer completed' or offer != 'Transaction With Offer' or offer_type != 'informational'
), dc as 
( -- filter data hanya dimana data yang diambil ada data dalam c Yang tidak terkontaminasi data yang di r
select c.* from c
where   
--    customer_id = '00cf471ed1aa42a8bdde5561d67da2b1' AND
     NOT EXISTS (
        SELECT 1
        FROM r 
        WHERE 
		--r.new_id = c.new_id 
		--or 
		(r.customer_id = c.customer_id AND r.OfferId = c.OfferId)  

    )  --and event != 'offer viewed' 
--order by customer_id,time
)-- setelah dilihat ternyata datanya masih ada yg terkontaminasi data yg di r maka akan difilter lagi yaitu data pada dc difilter dengan data yang di r yang mana new_id nya samaan
, kk as (
select dc.* from dc
where NOT EXISTS ( SELECT 1 FROM r WHERE r.new_id = dc.new_id  ) --and
--offer != 'Transaction With Offer' 
 --customer_id = '1d34870285e9470bac93313e3ae6d381' 

)
, gabung as (
select 
[offer]
      ,[customer_id]
      ,[event]
      ,[time]
      ,[Amount]
      ,[OfferId]
      ,[reward_get]
      ,[Tahun]
      ,[Bulan]
      ,[Tanggal]
      ,[became_member_on]
      ,[gender]
      ,[age]
      ,[income]
      ,[offer_type]
      --,[difficulty]
      --,[reward_offer]
      --,[duration]
      --,[channels]
from kk
--where offer = 'Transaction With Offer' 
--and customer_id = '28d2a4892f5b42c7afd7583c26fbfe21'
where event = 'offer received'
union all 
SELECT 
		[offer]
      ,[customer_id]
      ,[event]
      ,[time]
      ,[Amount]
      ,[OfferId]
      ,[reward_get]
      ,[Tahun]
      ,[Bulan]
      ,[Tanggal]
      ,[became_member_on]
      ,[gender]
      ,[age]
      ,[income]
      ,[offer_type]
      --,[difficulty]
      --,[reward_offer]
      --,[duration]
      --,[channels]
  FROM [ads].[dbo].[new_data]
  where offer = 'Transaction With Offer' 
  )
  select 
  g.*, ofs.[difficulty]
      ,ofs.[reward]
      ,ofs.[duration]
      ,ofs.[channels]
	  , CASE 
        WHEN [OfferId] ='ae264e3637204a6fb9bb56bc8210ddfd' THEN '1'
        WHEN [OfferId] = '4d5c57ea9a6940dd891ad53e9dbe8da0' THEN '2'
        WHEN [OfferId] = '9b98b8c7a33c4b65b9aebfe6a799e6d9'THEN '3'
        WHEN [OfferId] = 'f19421c1d4aa40978ebb69ca19b0e20d' THEN '4'
        WHEN [OfferId] = '0b1e1539f2cc45b7b9fa7c272da2e1d7' THEN '5'
        WHEN [OfferId] = '2298d6c36e964ae4a3e7e9706d1fb8c2' THEN '6'
        WHEN [OfferId] =  'fafdcd668e3743c1bb461111dcafc2a4'THEN '7'
        WHEN [OfferId] = '2906b810c7d4411798c6938adc9daaa5' THEN '8'
        ELSE 'Unknown Group'
    END AS groups
 -- count(*)
  from gabung as g
  inner join [ads].[dbo].[offers] as ofs
  on g.[OfferId] = ofs.[offer_id]
--  group by OfferId,event,offer_type
--  order by OfferId,event