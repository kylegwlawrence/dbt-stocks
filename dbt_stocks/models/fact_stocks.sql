{{ config(materialized='table') }}

WITH yest_row_num AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY business_date desc) row_num
  FROM {{ ref('stocks') }}
  )

, cte as (
select 
	ticker
    , business_date 
    , open as open_price
    , close as close_price
    , adjusted_close as adjusted_close_price
    , volume
    , low as low_price
    , high as high_price
    , row_num
from yest_row_num
)
select 
	main.ticker
    , main.business_date
    , cte.business_date as yest_business_date
    , round(main.open_price,2) as open_price
    , round(cte.open_price,2) as yest_open_price
    , round((main.open_price - cte.open_price),2) as delta_open_price
    , round(main.close_price,2) as close_price
    , round(cte.close_price,2) as yest_close_price
    , round((main.close_price - cte.close_price),2) as delta_close_price
    , round(((main.close_price - cte.close_price)/cte.close_price),2) as delta_perc_close_price
    , round(main.adjusted_close_price,2) as adjusted_close_price
    , round(cte.adjusted_close_price,2) as yest_adjusted_close_price
    , round((main.adjusted_close_price - cte.adjusted_close_price),2) as delta_close_price_adjusted
    , cast(round(main.volume) as integer) as volume
    , cast(round(cte.volume) as integer) as yest_volume
    , cast(round((main.volume - cte.volume)) as integer) as delta_volume
    , round(main.low_price,2) as low_price
    , round(cte.low_price,2) as yest_low_price
    , round((main.low_price - cte.low_price),2) as delta_low_price
    , round(main.high_price,2) as high_price
    , round(cte.high_price,2) as yest_high_price
    , round((main.high_price - cte.high_price),2) as delta_high_price
    , round((main.high_price - main.low_price),2) as high_low_spread
    , round((cte.high_price - cte.low_price),2) as yest_high_low_spread
    , round(((main.high_price - main.low_price) - (cte.high_price - cte.low_price)),2) as delta_high_low_spread
    , round((((main.high_price - main.low_price) - (cte.high_price - cte.low_price))/(cte.high_price - cte.low_price)),2) as delta_perc_high_low_spread
from cte main
full join cte
    on main.ticker = cte.ticker 
    and main.row_num = (cte.row_num - 1)
where main.ticker is not null