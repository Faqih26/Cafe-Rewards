--use ads ;
WITH b AS (
    SELECT
        *
    FROM ads.dbo.ADS_Transaction_OfferReceived
    WHERE offer_type != 'informational'
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
        [duration],
	ROW_NUMBER() OVER (PARTITION BY [customer_id], [OfferId] ORDER BY [time]) AS offer_sequence
FROM c
--where [OfferId] is not null and le_o is null
-- where [OfferId]= 'fafdcd668e3743c1bb461111dcafc2a4'
)
, CustomerEvents AS (
  SELECT
    customer_id,
	[OfferId],[offer_type],
    event,
    time,
    LEAD(
		case when [event] = 'transaction' then time end
		) 
			OVER (PARTITION BY customer_id ORDER BY time) AS next_event_time
  FROM
    d
)
SELECT
  customer_id,[OfferId],[offer_type],
  time AS offer_received_time,
  next_event_time AS first_transaction_time,
  next_event_time - time AS time_difference
FROM
  CustomerEvents
WHERE
  event = 'offer received'
  AND next_event_time IS NOT NULL
--  and customer_id = '0009655768c64bdeb2e877511632db8f'
order by customer_id,offer_received_time, offerID