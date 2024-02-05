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

, main_cte as (
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
    , nm.value as company_name
    , main.ticker || ': ' || nm.value as ticker_company_name
    , sctr.value as sector
    , ex.value as exchange
    , co.value as country
    , ad.value as address
    , cast(mcap.value as bigint) as market_cap_latest
    , cast(lq.value as date) as latest_quarter_end
    , round(cast(pm.value as numeric),3) as profit_margin
    , round(cast(atp.value as numeric),2) as analyst_target_price
    , round(cast(bt.value as numeric),2) as beta
from cte main
full join cte
    on main.ticker = cte.ticker 
    and main.row_num = (cte.row_num - 1)
left join {{ ref('overview') }} mcap on mcap.ticker = main.ticker and mcap.metric='MarketCapitalization'
left join {{ ref('overview') }} lq on lq.ticker = main.ticker and lq.metric='LatestQuarter'
left join {{ ref('overview') }} nm on nm.ticker = main.ticker and nm.metric='Name'
left join {{ ref('overview') }} sctr on sctr.ticker = main.ticker and sctr.metric='Sector'
left join {{ ref('overview') }} ex on ex.ticker = main.ticker and ex.metric='Exchange'
left join {{ ref('overview') }} co on co.ticker = main.ticker and co.metric='Country'
left join {{ ref('overview') }} ad on ad.ticker = main.ticker and ad.metric='Address'
left join {{ ref('overview') }} pm on pm.ticker = main.ticker and pm.metric='ProfitMargin'
left join {{ ref('overview') }} atp on atp.ticker = main.ticker and atp.metric='AnalystTargetPrice'
left join {{ ref('overview') }} bt on bt.ticker = main.ticker and bt.metric='Beta'
where main.ticker is not null
)

select 
    ticker
    , company_name
    , ticker_company_name
    , sector
    , exchange
    , country
    , address
    , market_cap_latest
    , latest_quarter_end
    , profit_margin
    , analyst_target_price
    , beta
    , business_date
    , yest_business_date
    , open_price
    , close_price
    , adjusted_close_price
    , volume
    , low_price
    , high_price
    , yest_open_price
    , yest_close_price
    , yest_adjusted_close_price
    , yest_volume
    , yest_low_price
    , yest_high_price
    , delta_open_price
    , delta_close_price
    , delta_close_price_adjusted
    , delta_volume
    , delta_low_price
    , delta_high_price
    , delta_high_low_spread
    , delta_perc_close_price
    , delta_perc_high_low_spread
from main_cte
