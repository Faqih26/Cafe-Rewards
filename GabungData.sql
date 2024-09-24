--use ads;
--CREATE OR ALTER VIEW [dbo].new_data AS
WITH new_dataset AS (
    SELECT
        e.[customer_id],
        e.[event],
        e.[time],
       -- ROW_NUMBER() OVER (PARTITION BY e.customer_id, e.time ORDER BY e.customer_id) AS v,
        e.[Amount],
        e.[OfferId],
        LEAD(e.[OfferId]) OVER (PARTITION BY e.customer_id,e.[time] ORDER BY e.[time])  AS le,
        LAG(e.[OfferId]) OVER (PARTITION BY e.customer_id,e.[time] ORDER BY e.[time])AS la,
		LEAD(o.offer_type) OVER (PARTITION BY e.customer_id,e.[time] ORDER BY e.[time])  AS le_of,
        LAG(o.offer_type) OVER (PARTITION BY e.customer_id,e.[time] ORDER BY e.[time]) AS la_of,
        e.[Reward] AS reward_get,
		c.[Tahun],
		c.[Bulan],
		c.[Tanggal],
        c.became_member_on,
        c.gender,
        c.age,
        c.income,
        o.offer_type,
        o.difficulty,
        o.reward AS reward_offer,
        o.duration,
        o.channels

    FROM [ads].[dbo].[events] AS e
    LEFT JOIN [ads].[dbo].[customers_new] AS c
        ON e.customer_id = c.customer_id
    LEFT JOIN [ads].[dbo].[offers] AS o
        ON o.[offer_id] = e.[OfferId]
)
, c as (
	SELECT
		COALESCE([OfferId],le, la  ) AS new_offer_id,
		[OfferId],
		le,
		la,
		COALESCE(offer_type,le_of, la_of  ) AS new_offer_type,
		offer_type,
		le_of,
		la_of,
		[customer_id],
		[event],
		[time],
		--case when offer_type <>'informational'  then  duration * 24 end as duration_jam,
		[Amount],
		case
			when 
				COALESCE([OfferId], le, la)  is null and [event] = 'transaction'  then 'Transaction Without Offer'
			when 
				[event] <> 'transaction'  then 'Offer'
				else 'Transaction With Offer' 
				end as offer,
		reward_get,
		[Tahun],
		[Bulan],
		[Tanggal],
		became_member_on,
		gender,
		age,
		income,
		difficulty,
		reward_offer,
		duration,
		channels
from new_dataset
)
select 
	[OfferId],
		le,
		la,
	case when (new_offer_id = '3f207df678b143eea3cee63160fa8bed' or new_offer_id = '5a8bc65990b245e5a138643cd4eb9837') and event = 'transaction' then NULL else new_offer_id end as new_offer_id,
	new_offer_type,
	offer_type,le_of,la_of,
	[customer_id],
		[event],
		[time],
		--case when offer_type <>'informational'  then  duration * 24 end as duration_jam,
		[Amount],
		offer,
		reward_get,
		[Tahun],
      [Bulan],
      [Tanggal],
		became_member_on,
		gender,
		age,
		income,
		difficulty,
		reward_offer,
		duration,
		channels
from c
--where new_offer_type = 'informational' and event= 'transaction'