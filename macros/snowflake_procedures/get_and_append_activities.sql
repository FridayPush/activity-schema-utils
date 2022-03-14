-- Developer TODO (1 of 3): Set the role which the procedure will execute commands with. 
--                          Note that the role will need privileges to create tables in locations specified by the `output_table` parameter.
use role analytics_admin;

-- Developer TODO (2 of 3): Set the database and schema for the procedure where the procedure will be created.
use database analytics;
use schema public;

create or replace procedure analytics.public.get_and_append_activities(
    INPUT_TEXT varchar
)
returns varchar
language javascript
as
$$

// Developer TODO (3 of 3): Update the constant below with the location of your activity schemas.
//                          Activity schemas are assumed to all reside inside the same SQL schema/folder.
const ACTIVITY_SCHEMA_LOCATION = "analytics.public";


try {
    var INPUT = JSON.parse(INPUT_TEXT);
}
catch {
    return "Error parsing JSON input - try pasting the text inbetween the single quotes at jsonlint.com"
}



// 1. GET METADATA FOR SOURCE ACTIVITY

var feature_1_name = "feature_1";
var feature_2_name = "feature_2"; 
var feature_3_name = "feature_3";

var metadata_query_text = 
    "select distinct \n" +
    "   feature_1, feature_2, feature_3 \n" +
    "from " + ACTIVITY_SCHEMA_LOCATION + "." + INPUT.entity + "_activity_dictionary \n" +
    "where activity = '" + INPUT.get_activity + "'" ;

var metadata_query_results = snowflake.createStatement({sqlText:metadata_query_text}).execute();

if (metadata_query_results.getRowCount() == 1)
{
    metadata_query_results.next();
    feature_1_name = metadata_query_results.getColumnValue(1);
    feature_2_name = metadata_query_results.getColumnValue(2);
    feature_3_name = metadata_query_results.getColumnValue(3);
}

var queried_activities_list = "'" + INPUT.get_activity + "'";
if ("append_activities" in INPUT) 
    for (let i = 0; i < INPUT.append_activities.length; i++)
        queried_activities_list += ", '" + INPUT.append_activities[i].activity + "'";



// 2. BUILD QUERY FOR "GET" ACTIVITY

var query_text = 
    "create or replace table " + INPUT.output_table + " as \n\n" +
    "with all_activities as ( \n" +
    "   select * from " + ACTIVITY_SCHEMA_LOCATION + "." + INPUT.entity + "_activities \n" +
    "   where activity in (" + queried_activities_list + ")\n";
    
    
if ("filter" in INPUT != '') 
    query_text +=
    "       and " + INPUT.filter + " \n";

query_text += 
    ") \n\n";

query_text += 
    ", get_activities as ( \n" +
    "   select \n" +
    "       activity_id, \n" +
    "       ts, \n" +
    "       " + INPUT.entity + ", \n" +
    "       activity, \n";

var existing_columns = ["activity_id", "ts", INPUT.entity, "activity"];
         
if (feature_1_name != "" && feature_1_name) {
    query_text += 
    "       feature_1 as " + feature_1_name + ",\n";
    existing_columns.push(feature_1_name);
}

if (feature_2_name != "" && feature_2_name) {
    query_text += 
    "       feature_2 as " + feature_2_name + ",\n";
    existing_columns.push(feature_2_name);
}

if (feature_3_name != "" && feature_3_name) {
    query_text += 
    "       feature_3 as " + feature_3_name + ",\n"; 
    existing_columns.push(feature_3_name);
}

query_text +=
    "       activity_occurrence, \n" +
    "       activity_repeated_at \n" +
    "   from all_activities \n" +
    "   where activity = '" + INPUT.get_activity + "' \n";
    
existing_columns.push("activity_occurrence", "activity_repeated_at")

if (INPUT.occurrences == "first")
    query_text +=
    "       and activity_occurrence = 1 \n";
else if (INPUT.occurrences == "last")
    query_text +=
    "       and activity_repeated_at is null \n";   
else if (INPUT.occurrences == "all")
    query_text +=
    "       and 1=1 \n"
else 
    query_text +=
    "       and activity_occurrence = " + INPUT.occurrences + " \n";

query_text += 
") \n\n";



// 3. ADD JOINS FOR APPENDED ACTIVITIES
    
if ("append_activities" in INPUT) 
{

    for (let i = 0; i < INPUT.append_activities.length; i++) {
    
        var APPEND_INPUT = INPUT.append_activities[i];
        
        if (["first_ever", "last_ever", "first_before", "last_before", "last_after", "first_inbetween", "last_inbetween"].includes(APPEND_INPUT.append_rule)) 
        {
    
            var append_feature_1_name = "appended_feature_1";
            var append_feature_2_name = "appended_feature_2"; 
            var append_feature_3_name = "appended_feature_3";

            metadata_query_text = 
                "select distinct \n" +
                "   feature_1, feature_2, feature_3 \n" +
                "from " + ACTIVITY_SCHEMA_LOCATION + "." + INPUT.entity + "_activity_dictionary \n" +
                "where activity = '" + APPEND_INPUT.activity + "'" ;

            metadata_query_results = snowflake.createStatement({sqlText:metadata_query_text}).execute();

            if (metadata_query_results.getRowCount() == 1)
            {
                metadata_query_results.next();
                append_feature_1_name = metadata_query_results.getColumnValue(1);
                append_feature_2_name = metadata_query_results.getColumnValue(2);
                append_feature_3_name = metadata_query_results.getColumnValue(3);
            }

            if (!!append_feature_1_name && existing_columns.includes(append_feature_1_name)) append_feature_1_name = APPEND_INPUT.append_rule + "_" + APPEND_INPUT.activity + "_" + append_feature_1_name;
            if (!!append_feature_2_name && existing_columns.includes(append_feature_2_name)) append_feature_2_name = APPEND_INPUT.append_rule + "_" + APPEND_INPUT.activity + "_" + append_feature_2_name;
            if (!!append_feature_3_name && existing_columns.includes(append_feature_3_name)) append_feature_3_name = APPEND_INPUT.append_rule + "_" + APPEND_INPUT.activity + "_" + append_feature_3_name;

            query_text +=
            ", append_activity_" + (i+1) + " as ( \n" +
            "   select \n" +
            "       primary.*, \n" +
            "       appended.ts as " + APPEND_INPUT.append_rule + "_" + APPEND_INPUT.activity + "_ts \n";
            existing_columns.push(APPEND_INPUT.append_rule + "_" + APPEND_INPUT.activity + "_ts")

            if (append_feature_1_name != "" && append_feature_1_name) {
                query_text += 
                "       ,appended.feature_1 as " + append_feature_1_name + "\n";
                existing_columns.push(append_feature_1_name);
            }

            if (append_feature_2_name != "" && append_feature_2_name) {
                query_text += 
                "       ,appended.feature_2 as " + append_feature_2_name + "\n";
                existing_columns.push(append_feature_2_name);
            }

            if (append_feature_3_name != "" && append_feature_3_name) {
                query_text += 
                "       ,appended.feature_3 as " + append_feature_3_name + "\n";
                existing_columns.push(append_feature_3_name);
            }

            if (i == 0) query_text +=
            "   from get_activities as primary \n";
            else  query_text +=
            "   from append_activity_" + (i) + " as primary \n";

            if (APPEND_INPUT.append_rule == "first_ever" || APPEND_INPUT.append_rule == "last_ever")
            {
                var append_filter = "appended.activity_occurrence = 1";
                if (APPEND_INPUT.append_rule == "last_ever") append_filter = "appended.activity_repeated_at is null";

                query_text +=
                "       left join all_activities as appended \n" +
                "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
                "           and appended." + INPUT.entity + " = primary." + INPUT.entity + " \n" +
                "           and " + append_filter + "\n";

            }
            else if (APPEND_INPUT.append_rule == "first_before") 
            {
                query_text += 
                "       left join all_activities as appended \n" +
                "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
                "           and appended." + INPUT.entity + " = primary." + INPUT.entity + " \n" +
                "           and appended.activity_occurrence = 1 \n" +
                "           and appended.ts < primary.ts \n";
            }
            else if (APPEND_INPUT.append_rule == "last_before")
            {
                query_text += 
                "       left join all_activities as appended \n" +
                "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
                "           and appended." + INPUT.entity + " = primary." + INPUT.entity + " \n" +
                "           and appended.ts < primary.ts and primary.ts <= appended.activity_repeated_at \n";
            } 
            else if (APPEND_INPUT.append_rule == "last_after")
            {
                query_text += 
                "       left join all_activities as appended \n" +
                "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
                "           and appended." + INPUT.entity + " = primary." + INPUT.entity + " \n" +
                "           and appended.activity_repeated_at is null \n" +
                "           and appended.ts > primary.ts \n";
            }
            else if (APPEND_INPUT.append_rule == "first_inbetween" || APPEND_INPUT.append_rule == "last_inbetween")
            {
                var sort_desc = ""
                if (APPEND_INPUT.append_rule == "last_inbetween") sort_desc = "desc"

                query_text +=
                "       left join all_activities as appended \n" +
                "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
                "           and appended." + INPUT.entity + " = primary." + INPUT.entity + " \n" +
                "           and appended.ts between primary.ts \n" +
                "                             and coalesce(primary.activity_repeated_at, '9999-12-31') \n";

                query_text +=
                "   qualify row_number() over (partition by primary.activity_id \n" +
                "                               order by appended.activity_occurrence " + sort_desc + ") = 1\n";
            }


            query_text +=
            ")\n\n";
        }
        else if (["aggregate_before", "aggregate_inbetween", "aggregate_after", "aggregate_all_ever", "aggregate"].includes(APPEND_INPUT.append_rule))
        {
            var aggregate_rule_suffix = APPEND_INPUT.append_rule.split("_")[1];
        
            query_text +=
            ", append_activity_" + (i+1) + " as ( \n" +
            "   select \n" +
            "       primary.*, \n" +
            "       count(appended.ts) as count_" + APPEND_INPUT.activity + "_" + aggregate_rule_suffix + " \n";

            if ("aggregation_function" in APPEND_INPUT)
                query_text +=
            "       ," + APPEND_INPUT.aggregation_function + " \n";

            if (i == 0) query_text +=
            "   from get_activities as primary \n";
            else  query_text +=
            "   from append_activity_" + (i) + " as primary \n";

            if (APPEND_INPUT.append_rule == "aggregate_before")
            {
                query_text +=
                "       left join all_activities as appended \n" +
                "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
                "           and appended." + INPUT.entity + " = primary." + INPUT.entity + " \n" +
                "           and appended.ts < primary.ts \n"; 
            } 
            else if (APPEND_INPUT.append_rule == "aggregate_inbetween")
            {
                query_text +=
                "       left join all_activities as appended \n" +
                "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
                "           and appended." + INPUT.entity + " = primary." + INPUT.entity + " \n" +
                "           and appended.ts between primary.ts \n" +
                "                             and coalesce(primary.activity_repeated_at, '9999-12-31') \n";
            } 
            else if (APPEND_INPUT.append_rule == "aggregate_after")
            {
                query_text +=
                "       left join all_activities as appended \n" +
                "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
                "           and appended." + INPUT.entity + " = primary." + INPUT.entity + " \n" +
                "           and appended.ts > primary.ts \n";
            } 
            else if (APPEND_INPUT.append_rule == "aggregate_all_ever")
            {
                query_text +=
                "       left join all_activities as appended \n" +
                "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
                "           and appended." + INPUT.entity + " = primary." + INPUT.entity + " \n";
            } 
            
            
            query_text += "\n   group by ";
            
            for (column_index = 0; column_index < existing_columns.length; column_index++)
            {
                query_text += "primary." + existing_columns[column_index];
                if (column_index < existing_columns.length - 1)
                    query_text += ', ';
                else
                    query_text += '\n';
            }
            
            existing_columns.push("count_" + APPEND_INPUT.activity + "_" + aggregate_rule_suffix);
            
            query_text +=
            ")\n\n";
        }
    }
    
    query_text +=
    "select * from append_activity_" + INPUT.append_activities.length + " ;"; 
}
else 
{
    query_text += 
    "select * from get_activities";
}

var query_statement = snowflake.createStatement({sqlText:query_text});
var query_results = query_statement.execute();

return "-- " + INPUT.output_table +" created.\n-- Query:- \n" + query_text;
    
$$;;