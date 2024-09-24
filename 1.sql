WITH OfferTransactions AS (
    SELECT
        customer_id,
        OfferId
    FROM [ads].[dbo].[new_data]
    WHERE offer = 'Transaction With Offer' or offer_type = 'informational' 
)
SELECT
    d.*
FROM [ads].[dbo].[new_data] d
LEFT JOIN OfferTransactions ot
ON d.customer_id = ot.customer_id AND d.OfferId = ot.OfferId
WHERE ot.customer_id IS NULL AND ot.OfferId IS NULL
order by customer_id,time ;