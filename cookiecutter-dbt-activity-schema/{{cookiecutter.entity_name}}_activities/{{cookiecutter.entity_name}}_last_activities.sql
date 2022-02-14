-- Bookmark the last activity for each customer so that for the next incremental run we can:
-- 1. Increment the activity occurrence field for activities created since the last run.
-- 2. Update the activity_repeated_at of the last activity with the first ts from the newly created activities

{{"{{"}}
  config(
    materialized = 'view'
    )
}}

select 
  
  activity_id as last_activity_id,
  ts,
  customer,
  activity,
  anonymous_customer_id,
  feature_1,
  feature_2,
  feature_3,
  revenue_impact,
  link,
  activity_occurrence,
  _activity_source

from {{"{{"}} ref('{{cookiecutter.entity_name}}_activities') }}

where activity_repeated_at is null