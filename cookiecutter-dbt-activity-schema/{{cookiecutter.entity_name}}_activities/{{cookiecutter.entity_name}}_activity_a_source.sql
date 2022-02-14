{{"{{"}}
  config(
    materialized = 'ephemeral',
    )
}}

{{"{#"}} Add old transformation version identifiers to the post hook in {{cookiecutter.entity_name}}_activities.sql to ensure the old data gets deleted #}
{{"{%"}} set activity_transformation_version = 'activity_a_transformation_v0.1' %}

select 
    id as activity_id,  
    timestamp as ts,
    {{cookiecutter.entity_name}}_id as customer,   
    null as anonymous_customer_id,
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

    and timestamp >= coalesce(

                        (select 
                            max(ts) 
                            from {{"{{"}} adapter.get_relation(this.database, this.schema, '{{cookiecutter.entity_name}}_activities') }}
                            where _activity_source = {{"'{{"}} activity_transformation_version }}'), 
                            
                        '1900-01-01'
                     )
    {{"{%"}} endif %}