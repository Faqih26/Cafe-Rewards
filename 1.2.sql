WITH OfferTransactions AS (
    SELECT
        customer_id,
        OfferId
    FROM [ads].[dbo].[new_data]
    WHERE offer = 'Transaction With Offer' or offer_type = 'informational'
)
SELECT
    *
FROM [ads].[dbo].[new_data] d
WHERE NOT EXISTS (
    SELECT 1
    FROM OfferTransactions ot
    WHERE d.customer_id = ot.customer_id AND d.OfferId = ot.OfferId
)
order by customer_id,time ; 
