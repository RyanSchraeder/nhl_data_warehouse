{{ config 
    (
        alias = 'seasonal_metrics_agg'
    )
}}

with src1 as (select * from {{ ref('games') }}), 
    src2 as (select * from {{ ref('teams') }} )

select * 
from src1 
join src2 
    on src1.visitor = sr2.team
union 
select * 
from src1 
join src2 
    on src1.home = sr2.team