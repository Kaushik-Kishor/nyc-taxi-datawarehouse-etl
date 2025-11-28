{{ config(materialized='table', schema='warehouse') }}

select 0 as rate_code_id, 'Unknown' as rate_code_description
union all
select 1, 'Standard rate'
union all
select 2, 'JFK'
union all
select 3, 'Newark'
union all
select 4, 'Nassau/Westchester'
union all
select 5, 'Negotiated fare'
union all
select 6, 'Group ride'
union all
select 99, 'Invalid / Misc Code'
