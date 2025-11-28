{{ config(
    materialized='table',
    schema='warehouse'
) }}

select 1 as vendor_id, 'Creative Mobile Technologies (CMT)' as vendor_name
union all
select 2, 'VeriFone Inc.' as vendor_name
