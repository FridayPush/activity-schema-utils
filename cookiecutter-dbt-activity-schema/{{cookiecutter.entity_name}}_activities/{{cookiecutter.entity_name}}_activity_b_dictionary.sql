{{"{#"}}
    Create dictionary entries for activities to enable SQL users and the tools/functions they use to understand the contents of your activity schemas.
#}

{{"{{"}}
  config(
    materialized = 'ephemeral',
    )
}}

select
                                                            -- Examples: -
    'activity_b' as activity,                               --   web_session_started
    'Description of activity b' as activity_description,    --   User started a new web session.

    'field_name_for_feature_1' as feature_1,                --   browser
    'SQL DATA TYPE'            as feature_1_data_type,      --   TEXT
    'Description of feature 1' as feature_1_description,    --   Browser that user started sessions with, e.g. IE, Firefox, Chrome.

    'field_name_for_feature_2' as feature_2,                --   browser_window_width
    'SQL DATA TYPE'            as feature_2_data_type,      --   NUMBER
    'Description of feature 2' as feature_2_description,    --   Horizontal width of browser windown that session was started with.

    'field_name_for_feature_3' as feature_3,                --   device
    'SQL DATA TYPE'            as feature_3_data_type,      --   TEXT
    'Description of feature 3' as feature_3_description     --   Description of device used for session.