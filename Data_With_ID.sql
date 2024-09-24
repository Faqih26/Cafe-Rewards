--use ads;
--CREATE OR ALTER VIEW [dbo].data_with_ID AS
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
, c as (
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
select 
	*
from c
