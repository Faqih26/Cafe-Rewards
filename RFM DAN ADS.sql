with b as (
SELECT [OfferId]
      ,[gender]
      ,[klasifikasi_age]
      ,[offer_type]
      ,[difficulty]
	  ,NTILE(5) OVER (partition by offer_type order by [avg_amount_spend] asc) as  diff_quantile_avgspendamount
      ,[rn_diff]
      ,[reward]
      ,[rn]
      ,[total]
      ,[avg_amount_spend]
  FROM [ads].[dbo].[klasifikasiforADS]
  )
  select 
  rfm.*
  ,b.*
  from b
  inner join  [ads].[dbo].[RFM] as rfm
  on rfm.klasifikasi_age = b.klasifikasi_age and b.diff_quantile_avgspendamount = rfm.monetary
  order by customer_id,offerid