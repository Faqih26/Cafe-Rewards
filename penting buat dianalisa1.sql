WITH b AS (
    SELECT
        CASE 
            WHEN final_offer_id = offerid OR final_offer_id IS NULL OR offerid IS NULL 
                THEN NULL 
            ELSE ROW_NUMBER() OVER (PARTITION BY customer_id, offerid ORDER BY time) 
        END AS rw,
        *
    FROM data_with_ID
   WHERE customer_id = 'a50ed51e4fda4d59ba643749469894c4'
	--and offer_type <> 'informational'
)
, c as (
SELECT
   rw,
   final_offer_id,
   RANK() over(partition by customer_id, offerID order by time) as id,
        [offer],
        [customer_id],
        [event],
        [time],
        MIN([time]) OVER (PARTITION BY customer_id,[OfferId] ORDER BY [time] ) AS min_time,--min time msh salah
		lead(time) over(partition by customer_id,offerid  order by time asc)  as tm, -- harusnya tm ini bandinginnya sama transaction bukan offer dikemudian hari
		coalesce (duration * 24 , lead(duration * 24 ) OVER (partition by customer_id,offerid ORDER BY [time])) as jamduration,
        [Amount],
        [OfferId],
        LEAD(OfferId) OVER (partition by customer_id ORDER BY [time]) AS le_o,
		--last_value(OfferId) ignore nulls over(order by [time]) new_past_due_col,
        [offer_type],
        [duration]
FROM b
)
,d as (
SELECT 
rw,
	id,
	 ROW_NUMBER() OVER (PARTITION BY [customer_id], [OfferId] ORDER BY [time]) AS offer_sequence,
	[offer],
        [customer_id],
        [event],
        [time],
		min_time,
		tm,
		tm - min_time as tmmintime,
		jamduration,
		case 
			when [OfferId] = le_o and event = 'offer received' and offer_type <> 'informational'
				then 'Get New Kupon'
			when jamduration < tm - min_time and [OfferId] is not null and le_o is null 
				then 'Offer Expired' 
			when [OfferId] = le_o and event = 'offer received' and jamduration < tm - min_time and [OfferId] is not null and le_o is null 
				then 'Expired and Get new'
		end as Kondisi_Kupon,
        [Amount],
        [OfferId],
		final_offer_id,
        le_o,
	--	new_past_due_col,
        [offer_type],
        [duration]
FROM c
--where [OfferId] is not null and le_o is null
-- where [OfferId]= 'fafdcd668e3743c1bb461111dcafc2a4'
)
select * from d