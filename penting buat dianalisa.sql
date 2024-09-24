--- bikin id 
WITH b AS (
    SELECT 
		RANK() over(partition by customer_id, offerID order by time) as id,
        [offer],
        [customer_id],
        [event],
        [time],
        MIN([time]) OVER (PARTITION BY customer_id,[OfferId] ORDER BY [time] ) AS min_time,--min time msh salah
		MIN([time]) OVER (PARTITION BY customer_id,final_offer_id ORDER BY [time] ) AS min_time2,--min time msh salah
		lead(time) over(partition by customer_id  order by time asc)  as tm,
		duration * 24 as jam_duration,
        [Amount],
        [OfferId],
		final_offer_id,
        LEAD(OfferId) OVER (partition by customer_id ORDER BY [time]) AS le_o,
		--last_value(OfferId) ignore nulls over(order by [time]) new_past_due_col,
        [offer_type],
        [duration]
    FROM [ads].[dbo].data_with_ID
    WHERE customer_id = '003d66b6608740288d6cc97a6903f4f0'
)
SELECT 
	id,
	 ROW_NUMBER() OVER (PARTITION BY [customer_id], [OfferId] ORDER BY [time]) AS offer_sequence,
	[offer],
        [customer_id],
        [event],
        [time],
		min_time,
		min_time2,
		tm,
		tm - min_time,
		jam_duration,
		case 
			when jam_duration < tm - min_time and [OfferId] is not null and le_o is null 
				then 'Offer Expired' 
			when [OfferId] = le_o and event = 'offer received'
				then 'Get New Kupon'
		end as Kondisi_Kupon,
        [Amount],
        [OfferId],
		final_offer_id,
        le_o,
	--	new_past_due_col,
        [offer_type],
        [duration]
FROM b
--where [OfferId] is not null and le_o is null
-- where [OfferId]= 'fafdcd668e3743c1bb461111dcafc2a4'
order by time,offerID