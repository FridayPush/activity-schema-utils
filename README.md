# activity-schema-utils

[comment]: <> (What are the activity schema utils? Why would you want to use them?)

This repo contains a set of utilities for creating and analysing activity schemas using [DBT](https://www.getdbt.com/). An [activity schema](https://www.activityschema.com/) enables a large number of questions about a data set to be answered with a small amount of data modelling. 


[comment]: <> (How do the utils fit into the broader ecosystem? What are the dependencies?)

The utilities implement the [activity schema spec](https://github.com/ActivitySchema/ActivitySchema/blob/main/spec.md) pioneered by the team at [Narrator.ai](https://www.narrator.ai/). Narrator is a fully managed solution for creating activity schemas enabling all business users (not just SQL users) to analyse data. This repo is focused on the smaller problem of letting organisations already using DBT enable their SQL users to take advantage of activity schemas.

This repo is organised into two folders:

- `cookiecutter-dbt-activity-schema/` is a cookiecutter template for adding activity schema models to an existing DBT project. These models can be configured to update incrementally enabling large datasets to be modelled a lower cost. 
  
- `macros/` contains a set of macros enabling SQL users answer questions using activity schemas. These macros enable even those who are relatively new to SQL to do sophisticated analyses (for example cohort analysis). Currently the macros are written as snowflake stored procedures but in the near future will be converted into DBT macros to take advantage of the upcoming DBT server feature.

## cookiecutter-dbt-activity-schema/

The cookiecutter templates makes it easy to add a new activity schema to a DBT project. The steps below walk through how to add a new model and then how to add new activities to the model:

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

## macros/

To do:
- [ ] Describe how to install and use snowflake procedures
