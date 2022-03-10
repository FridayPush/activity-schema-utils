{{"{{"}}
  config(
    materialized = 'incremental',
    unique_key='activity_id',

    pre_hook={{"{%"}} if is_incremental() %}

              -- Remove records from old versions of activity transformations.
              BEGIN; 
              delete from {{"{{"}} this }} 
              where _activity_source in ('old_version_of_activity_transformation_to_delete_v0.1',
                                         'old_version_of_activity_transformation_to_delete_v0.2') ;
              COMMIT;
              
              {{"{%"}} else %}

              -- If a full refresh then do nothing (but a SQL placeholder is necessary)
              select 1; 

              {{"{%"}} endif %}"
  )
}}
{{"{#"}} Consider adding clustering to the above config to improve performance - e.g. cluster_by=['to_date(ts)'] #}


with new_activities as (

  select 
      *,

      row_number() 
          over (partition by {{cookiecutter.entity_name}}, 
                             activity
                order by ts, activity_id) as activity_occurrence,

      lead(ts) 
          over (partition by {{cookiecutter.entity_name}}, 
                             activity
                order by ts, activity_id) as activity_repeated_at

  from {{"{{"}} ref('union_{{cookiecutter.entity_name}}_activities') }} 

)


{{"{%"}} if not is_incremental() %}
  
  select * from new_activities

{{"{%"}} else %}

  -- get the first existing activity that is also present in the new activities
  , first_of_activities_to_be_updated as (

    select 
      existing_activities.{{cookiecutter.entity_name}},
      existing_activities.activity,
      existing_activities.activity_occurrence 

    from new_activities

      join {{"{{"}} this }} as existing_activities
      
        on new_activities.{{cookiecutter.entity_name}} = existing_activities.{{cookiecutter.entity_name}}     -- Match on {{cookiecutter.entity_name}} and activity to aid optimisation of join.
          and new_activities.activity = existing_activities.activity  -- ... Can be removed if not helpful.
          and new_activities.activity_id = existing_activities.activity_id

    qualify row_number() over (partition by existing_activities.{{cookiecutter.entity_name}}, existing_activities.activity
                               order by existing_activities.activity_occurrence) = 1

  )

  , last_existing_activity as (

    select 
      *
    from {{"{{"}} this }}
    where activity_repeated_at is null

  )

  -- 1. Increment activity occurrence of newly created activities based occurrences in existing data.
  select

      new_activities.activity_id,
      new_activities.ts,
      new_activities.{{cookiecutter.entity_name}},
      new_activities.activity,
      new_activities.anonymous_{{cookiecutter.entity_name}}_id,
      new_activities.feature_1,
      new_activities.feature_2,
      new_activities.feature_3,
      new_activities.revenue_impact,
      new_activities.link,

      new_activities.activity_occurrence
        + coalesce(first_of_activities_to_be_updated.activity_occurrence - 1,  -- If update overlaps with existing data then increment new occurrence counts
                                                                               -- ... based on the existing occurrence of the first activity which is to be updated by new data.

                   last_existing_activity.activity_occurrence,                  -- If update doesn't overlap then increment new occurrences based on the occurrence of latest activity in the existing data.

                   0)                                                           -- For activities which are completely new to {{cookiecutter.entity_name}}s there's no need to increment the occurrence     
        as activity_occurrence,

      new_activities.activity_repeated_at,
      new_activities._activity_source

  from new_activities 

      left join first_of_activities_to_be_updated
          on new_activities.{{cookiecutter.entity_name}} = first_of_activities_to_be_updated.{{cookiecutter.entity_name}} 
              and new_activities.activity = first_of_activities_to_be_updated.activity 

      left join last_existing_activity
          on new_activities.{{cookiecutter.entity_name}} = last_existing_activity.{{cookiecutter.entity_name}} 
              and new_activities.activity = last_existing_activity.activity 

  union all 

  -- 2. If new data doesn't overlap with existing data (i.e. there are no activies to be updated)
  --    ... then update the activity_repeated_at of the last activity in the existing data set
  --    ... with the timestamp of the first activity from the new data.
  select 

      last_existing_activity.activity_id,
      last_existing_activity.ts,
      last_existing_activity.{{cookiecutter.entity_name}},
      last_existing_activity.activity,
      last_existing_activity.anonymous_{{cookiecutter.entity_name}}_id,
      last_existing_activity.feature_1,
      last_existing_activity.feature_2,
      last_existing_activity.feature_3,
      last_existing_activity.revenue_impact,
      last_existing_activity.link,
      last_existing_activity.activity_occurrence,
      new_activities.ts as activity_repeated_at,
      last_existing_activity._activity_source

  from last_existing_activity

      join new_activities
          on  last_existing_activity.{{cookiecutter.entity_name}} = new_activities.{{cookiecutter.entity_name}} 
          and last_existing_activity.activity = new_activities.activity 
              and new_activities.activity_occurrence = 1 


      left join first_of_activities_to_be_updated
          on last_existing_activity.{{cookiecutter.entity_name}} = first_of_activities_to_be_updated.{{cookiecutter.entity_name}} 
              and last_existing_activity.activity = first_of_activities_to_be_updated.activity 

  where first_of_activities_to_be_updated.{{cookiecutter.entity_name}} is null -- if this is null then there are no activities to be 
                                                      -- ... updated for this {{cookiecutter.entity_name}} and activity... i.e. no overlap.

{{"{%"}} endif %}