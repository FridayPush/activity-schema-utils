# activity-schema-utils


## Contents
- [Introduction](#introduction)
- [Create an activity schema](#create-an-activity-schema)
- [Analyse an activity schema](#analyse-an-activity-schema)
  - [Get and append activities](#get-and-append-activities)
  - [Macro input reference](#macro-input-reference)

## Introduction

[comment]: <> (What are the activity schema utils? Why would you want to use them?)

This repo contains a set of utilities for creating and analysing activity schemas using [DBT](https://www.getdbt.com/). An [activity schema](https://www.activityschema.com/) enables a large number of questions about a data set to be answered with a small amount of data modelling. 


[comment]: <> (How do the utils fit into the broader ecosystem? What are the dependencies?)

The utilities implement the [activity schema spec](https://github.com/ActivitySchema/ActivitySchema/blob/main/spec.md) pioneered by the team at [Narrator.ai](https://www.narrator.ai/). Narrator is a fully managed solution for creating activity schemas enabling all business users (not just SQL users) to analyse data. This repo is focused on the smaller problem of letting organisations already using DBT enable their SQL users to take advantage of activity schemas.

This repo is organised into two folders:

 - `cookiecutter-dbt-activity-schema/` is a cookiecutter template for adding activity schema models to an existing DBT project. These models can be configured to update incrementally enabling large datasets to be modelled a lower cost. 
  
 - `macros/` contains a set of macros enabling SQL users answer questions using activity schemas. These macros enable even those who are relatively new to SQL to do sophisticated analyses (for example cohort analysis). Currently the macros are written as snowflake stored procedures but in future will be converted into DBT macros.

## Create an activity schema

The cookiecutter template makes it easy to add a new activity schema to a DBT project. The steps below walk through how to create a new model and then how to add new activities to the model:

1. **Add an activity schema model to a DBT project:**
    1. Install cookiecutter ([installation guide](https://cookiecutter.readthedocs.io/en/latest/installation.html)).
    2. Open a terminal and navigate to the directory where you'd like the model to go (e.g. .../your_dbt_project/models/) and then run
       ```bash
       cookiecutter https://github.com/birdiecare/activity-schema-utils --directory="cookiecutter-dbt-activity-schema"
       ```
    3. Follow the command line prompts to input details about the new activity schema.

2. **Add a new activity to the schema:**
    1. Open the model file ending `..._activity_a_source.sql` - the file will be prefixed based on the entity name given during creation of the activity schema, for example `customer_activity_a_source.sql`. 
    2. Update the model based on the `-- Developer TODO:` comments contained in the file.
    3. Rename the model file to reflect the activity(s) created by the model.
    4. Update the `union_..._activites.sql` to reference the new name of the activity transformation. 
    5. Now try running the `..._activities` model (e.g. `customer_activities`) in DBT.


3. **Document the new activity:**
    1. Open the file ending `..._activity_a_dictionary.sql` (e.g. `customer_activity_a_dictionary.sql`). This model creates the metadata used by activity schema macros to output the correct field names and types for feature fields. It's also handy for SQL users to quickly get the list of activities contained in an activity schema.
    2. Update the model based on the `-- Developer TODO:` comments contained in the file.
    3. Rename the model file to reflect the activity(s) documented by the model.
    4. Update the `..._activity_dictionary.sql` model reference the new name of the activity dictionary.
    5. Now try running the `..._activity_dictionary` model (e.g. customer_activity_dictionary`) in DBT.

Congratulations! You now have an activity schema compatible with activity schema macros documented below.

## Analyse an activity schema

The macros enable sophisticated analyses without in-depth knowledge of SQL. Currently the macros are written as Snowflake procedures but in future will be converted to DBT macros enabling interactive analysis across multiple platform types using DBT server. The steps below walk through how to install and use the Snowflake procedures.

### Get and append activities

#### get_and_append_activities()

The `get_and_append_activities()` macro allows multiple activities to be appended to a primary activity using different relationships (there's a fantastic explanation of all the relationships here: https://docs.narrator.ai/docs/relationships). The outputted table puts all the information about an individual activity on a single row making it much easier for SQL users to calculate things like conversion rates or the time between two activities.

The step below walks through how to create and use the procedure in Snowflake.

1. **Install the get_and_append_activities() macro:**
    1. Open [get_and_append_activities.sql](https://github.com/birdiecare/activity-schema-utils/blob/main/macros/snowflake_procedures/get_and_append_activities.sql) in a SQL editor connected to Snowflake.
    2. Update the procedure based on the `-- Developer TODO:` comments contained in the file.
    3. Run each of the SQL commands in the file.

2. **Call the macro:**
    1. Choose two activities from your activity schema. In this example there are two activities, `customer_received_email` and `customer_responded_to_email`, from an activity schema called `customer_activities`.
    2. Update the `entity`, `get_activity`, `append_activities` fields in the query below to relect your activity schema:
        ```SQL
        call get_and_append_activities('{

            "entity": "customer", 

            "get_activity": "customer_received_email",

            "occurrences": "all",     

            "append_activities": [

                {
                    "activity": "customer_responded_to_email",
                    "append_relationship": "first_inbetween"
                }    
            ],

            "output_table": "analytics.output_schema.conversion_results_1"
        }');
        ```
    3. Submit the updated query and then run the query below (updating the `output_schema`) to view the outputted table:
        ```SQL
        select * from analytics.output_schema.conversion_results_1
        order by ts
        ```
    This table can be directly used to calculate conversion rates or times between activities.

#### show_appended_activities_example()

The `show_appended_activities_example()` macro helps SQL users understand how activities are appended by different relationships when using the `get_and_append_activities()` macro. The `linked_to_appended_activity_occurrence` field in the outputted table identifies the occurrence of the appended activity which the primary activity has been linked to. 

1. **Install the show_appended_activities_example() macro:**
    1. Open [show_appended_activities_example.sql](https://github.com/birdiecare/activity-schema-utils/blob/main/macros/snowflake_procedures/show_appended_activities_example.sql) in a SQL editor connected to Snowflake.
    2. Update the procedure based on the `-- Developer TODO:` comments contained in the file.
    3. Run each of the SQL commands in the file.

2. **Call the macro:**
    1. Choose two activities from your activity schema. In this example there are two activities, `customer_received_email` and `customer_responded_to_email`, from an activity schema called `customer_activities`.
    2. Update the `entity`, `get_activity`, `append_activities` fields in the query below to relect your activity schema:
        ```SQL
        call show_appended_activities_example('{

            "entity": "customer", 

            "get_activity": "customer_received_email",

            "occurrences": "all",     

            "append_activities": [

                {
                    "activity": "customer_responded_to_email",
                    "append_relationship": "first_inbetween"
                }    
            ],

            "output_table": "analytics.output_schema.appended_activities_example_1"
        }');
        ```
    3. Submit the updated query and then run the query below (updating the `output_schema`) to view the example:
        ```SQL
        select * from analytics.output_schema.appended_activities_example_1
        order by ts
        ```
### Macro input reference

Both macros described above takes the following JSON formatted inputs.
| Field    |      Description     |
|----------|-------------|
| entity |  Defines the activity schema to analyse, for example an input of `customer` points the macro at the `customer_activities` schema. |
| get_activity | The primary activity to be analyses. All other activities will be appended to this activity.  |  
| occurrences | Which occurrences of the primary activity to get. `first` gets earliest activity for a particular entity, `last` gets the latest, `all` gets all activities and `2`,`3`,`4` gets the second, third, fourth activity. |   
| append_activities | An array of key value pairs specifying the `activity` to be appended and the `append_relationship` to be applied when appending. The latter can take on the values: `first_ever`, `last_ever`, `first_before`, `last_before`, `last_after`, `first_inbetween`, `last_inbetween`, `aggregate_before`, `aggregate_after`, `aggregate_inbetween` and `aggregate_all_ever`. A great explanation of each of these relationships is available at https://docs.narrator.ai/docs/relationships. |
| output_table | The fully qualified location where the macro should output its results. |
