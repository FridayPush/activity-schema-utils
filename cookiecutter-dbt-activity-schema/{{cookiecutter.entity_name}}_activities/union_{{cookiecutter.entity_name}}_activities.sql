{{"{{"}}
  config(
    materialized = 'ephemeral',
    )
}}

select * from {{"{{"}} ref('{{cookiecutter.entity_name}}_activity_a_source') }}

-- union all

-- select * from {{"{{"}} ref('{{cookiecutter.entity_name}}_activity_b_source') }}