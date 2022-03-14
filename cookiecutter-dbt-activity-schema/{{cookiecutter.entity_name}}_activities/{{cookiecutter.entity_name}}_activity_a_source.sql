{{"{{"}}
  config(
    materialized = 'ephemeral',
    )
}}

-- Developer TODO: Update the activity version to include the name of the new activity: e.g. 'support_chat_started_transformation_v0.1'.
--                 In future you then can add old transformation version identifiers to the pre-hook of {{cookiecutter.entity_name}}_activities.sql ...
--                 ... to ensure records from previous transformation versions get deleted. This is only needed for incremental materialisations.
{{"{%"}} set activity_transformation_version = 'activity_a_transformation_v0.1' %} 


-- Developer TODO: Update the select statement to map your source data to the activity schema fields...
--                 Examples of transformations can be found at https://docs.narrator.ai/docs/by-data-source.
select 
    id as activity_id,  
    timestamp as ts,
    {{cookiecutter.entity_name}}_id as {{cookiecutter.entity_name}},   
    null as anonymous_{{cookiecutter.entity_name}}_id,
    'activity_a' as activity

    null as feature_1, 
    null as feature_2, 
    null as feature_3,
    null as revenue_impact,
    null as link,

    {{"'{{"}} activity_transformation_version }}' as _activity_source

from {{"{{"}} source('source_a', 'schema_a') }}

where 1=1

    {{"{%"}} if adapter.get_relation(this.database, this.schema, '{{cookiecutter.entity_name}}_activities') is not none 
            and not flags.FULL_REFRESH %}

    -- Developer TODO: Update this where clause to only source activities which don't already exist in the activity schema.
    --                 This is only needed for incremental materialisations.
    and timestamp >= coalesce(

                        (select 
                            max(ts) 
                            from {{"{{"}} adapter.get_relation(this.database, this.schema, '{{cookiecutter.entity_name}}_activities') }}
                            where _activity_source = {{"'{{"}} activity_transformation_version }}'), 
                            
                        '1900-01-01'
                     )
    {{"{%"}} endif %}