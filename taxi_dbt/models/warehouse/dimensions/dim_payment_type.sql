{{ config(
    materialized='table',
    schema='warehouse'
) }}

select 0 as payment_type_id, 'Unknown'        as payment_type_description
union all
select 1 as payment_type_id, 'Credit Card'
union all
select 2 as payment_type_id, 'Cash'
union all
select 3 as payment_type_id, 'No Charge'
union all
select 4 as payment_type_id, 'Dispute'
union all
select 5 as payment_type_id, 'Unknown Payment'
union all
select 6 as payment_type_id, 'Voided Trip'
