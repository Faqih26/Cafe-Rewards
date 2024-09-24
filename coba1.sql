--use ads;
--CREATE OR ALTER VIEW [dbo].cobacoba AS
WITH new AS (
    SELECT  
        [rw],
        [final_offer_id],
        [offer],
        [customer_id],
        [event],
        [OfferId],
        [offer_type],
        duration,
		duration * 24  as duration_hour,
        [time],
		time_transaction,
        CASE WHEN event = 'transaction' THEN time END AS time_transaction2
    FROM 
        [ads].[dbo].[ADS_Transaction_OfferReceived]
    WHERE  customer_id = '005500a7188546ff8a767329a2f7c76a'
)
, ca as (
SELECT 
    *
--   ,LAST_VALUE(time_transaction) OVER (PARTITION BY [customer_id] ORDER BY time_transaction RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS highest_salary
    ,LAST_VALUE(time_transaction) OVER (PARTITION BY [customer_id] ORDER BY time_transaction ROWS BETWEEN CURRENT ROW AND 4 FOLLOWING) AS highest_salary2
	,lead(CASE WHEN event = 'transaction' THEN time END) over(partition by customer_id order by time asc ) as t
--  ,  LAST_VALUE(time_transaction) OVER (PARTITION BY [customer_id] ORDER BY time_transaction ROWS 3 PRECEDING) AS highest_salary3
FROM 
    new 

)
select
*
,LAST_VALUE(t) OVER (PARTITION BY [customer_id] ORDER BY t RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS highest_salary
,coalesce(t, lead(t) over(order by t)) as cs
from ca
ORDER BY customer_id, time;
