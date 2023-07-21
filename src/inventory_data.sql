SELECT 
  a.timestamp, 
  a.product_id, 
  a.estimated_stock_pct, 
  b.quantity, 
  c.temperature 
FROM 
  stock_agg_df as a 
  LEFT JOIN sales_agg_df as b on a.product_id = b.product_id 
  and a.timestamp = b.timestamp 
  LEFT JOIN temp_agg_df as c ON a.timestamp = c.timestamp;