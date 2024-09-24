--use ads;
--CREATE OR ALTER VIEW [dbo].ADS_Transaction_OfferReceived AS -- untuk melihat kupon expired
WITH r AS (
    SELECT 
        CASE 
            WHEN CHARINDEX('_', final_offer_id) > 0 THEN 
                RIGHT(final_offer_id, LEN(final_offer_id) - CHARINDEX('_', final_offer_id))
            ELSE 
                NULL
        END AS rw,
        OfferId,
        customer_id
    FROM 
        [ads].[dbo].data_with_ID
    WHERE 
--        customer_id = '00cf471ed1aa42a8bdde5561d67da2b1' AND
         event = 'offer completed' or offer = 'Transaction With Offer' or offer_type = 'informational'
)
,c as (
SELECT 
    CASE 
        WHEN CHARINDEX('_', final_offer_id) > 0 THEN 
            RIGHT(final_offer_id, LEN(final_offer_id) - CHARINDEX('_', final_offer_id))
        ELSE 
            NULL
    END AS rw,
	case when event = 'transaction' then time else null end as time_transaction,
    nd.*
FROM 
    [ads].[dbo].data_with_ID AS nd
WHERE 
--    customer_id = '00cf471ed1aa42a8bdde5561d67da2b1' AND
     NOT EXISTS (
        SELECT 1
        FROM r ot
        WHERE ot.customer_id = nd.customer_id 
          AND ot.OfferId = nd.OfferId  

    ) and event != 'offer viewed' 
--order by customer_id,time
)
select * from c
where offer != 'Transaction With Offer'