{{"{{"}}
  config(
    materialized = 'incremental',
    unique_key='activity_id',

    post_hook=" -- Remove records from old versions of activity transformations.
                BEGIN; 
                delete from {{"{{"}} this }} 
                where _activity_source in ('old_version_of_activity_transformation_to_delete_v0.1',
                                           'old_version_of_activity_transformation_to_delete_v0.2') ;
                COMMIT;"
  )
}}
{{"{#"}} Consider adding clustering to the above config to improve performance - e.g. cluster_by=['to_date(ts)'] #}


with new_activities as (

  select 
      *,

      row_number() 
          over (partition by customer, 
                          activity
              order by ts) as activity_occurrence,

      lead(ts) 
          over (partition by customer, 
                          activity
              order by ts) as activity_repeated_at

  from {{"{{"}} ref('union_{{cookiecutter.entity_name}}_activities') }} 

)


{{"{%"}} if not is_incremental() %}
  
  select * from new_activities

{{"{%"}} else %}

  {{"{%"}} if adapter.get_relation(this.database, this.schema, '{{cookiecutter.entity_name}}_last_activities') is none %}
  {{"{%"}}- do log('{{cookiecutter.entity_name}}_last_activities model not available, please run this before starting an incremental load.', info=True) -%}
  {{"{%"}} endif %}

  -- 1. Increment activity occurrence of newly created activities based on last activity before load was started.
  select

      new_activities.activity_id,
      new_activities.ts,
      new_activities.customer,
      new_activities.activity,
      new_activities.anonymous_customer_id,
      new_activities.feature_1,
      new_activities.feature_2,
      new_activities.feature_3,
      new_activities.revenue_impact,
      new_activities.link,
      new_activities.activity_occurrence + coalesce(last_activities.activity_occurrence, 1) - 1 as activity_occurrence,
      new_activities.activity_repeated_at,
      new_activities._activity_source

  from new_activities 

      left join {{"{{"}} adapter.get_relation(this.database, this.schema, '{{cookiecutter.entity_name}}_last_activities') }} as last_activities

          on new_activities.customer = last_activities.customer 
              and new_activities.activity = last_activities.activity 

  where new_activities.activity_id != last_activities.last_activity_id -- exclude last_activity id (this will be inserted below)
    or last_activities.last_activity_id is null


  union all 

  -- 2. Find activity_repeated_at for last occurrence of activity before incremental update
  select 

      last_activities.last_activity_id as activity_id,
      last_activities.ts,
      last_activities.customer,
      last_activities.activity,
      last_activities.anonymous_customer_id,
      last_activities.feature_1,
      last_activities.feature_2,
      last_activities.feature_3,
      last_activities.revenue_impact,
      last_activities.link,
      last_activities.activity_occurrence,
      new_activities.ts as activity_repeated_at,
      last_activities._activity_source

  from {{"{{"}} adapter.get_relation(this.database, this.schema, '{{cookiecutter.entity_name}}_last_activities') }} as last_activities

      join new_activities 
          on  last_activities.customer = new_activities.customer 
          and last_activities.activity = new_activities.activity 
              and new_activities.activity_occurrence = 1

{{"{%"}} endif %}