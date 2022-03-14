{{"{{"}}
  config(
    materialized = 'ephemeral',
    )
}}

-- Developer TODO: Update the dictionary entry for the new activity. This metadata is then used by
--                 activity schema macros to output the correct field names and types for feature fields.
select
                                                            -- Examples: -
    'activity_a' as activity,                               --   support_chat_started
    'Description of activity a' as activity_description,    --   User started an intercom conversation with the support team.

    'field_name_for_feature_1' as feature_1,                --   first_responded_at
    'SQL DATA TYPE'            as feature_1_data_type,      --   TIMESTAMP
    'Description of feature 1' as feature_1_description,    --   Timestamp recording when conversation was first responded to by support team.

    'field_name_for_feature_2' as feature_2,                --   support_responses_count
    'SQL DATA TYPE'            as feature_2_data_type,      --   NUMBER
    'Description of feature 2' as feature_2_description,    --   Count of total responses from the support team require to resolve chat.

    'field_name_for_feature_3' as feature_3,                --   satisfaction rating
    'SQL DATA TYPE'            as feature_3_data_type,      --   TEXT
    'Description of feature 3' as feature_3_description     --   User rating of the support they received, on a scale from 'Terrible' to 'Amazing'.