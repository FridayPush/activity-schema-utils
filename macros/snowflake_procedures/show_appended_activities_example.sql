-- Developer TODO (1 of 3): Set the role which the procedure will execute commands with. 
--                          Note that the role will need privileges to create tables in locations specified by the `output_table` parameter.
use role analytics_admin;

-- Developer TODO (2 of 3): Set the database and schema for the procedure where the procedure will be created.
use database analytics;
use schema public;


create or replace procedure show_appended_activities_example(
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

if (!"append_activities" in INPUT)
    return "One appended activity is required for example.";
    
if (INPUT.append_activities.length > 1)
    return "Only one appended activity is required for example.";
    
var APPEND_INPUT = INPUT.append_activities[0];

// 1. FIND ONE EXAMPLE ENTITY (a good example should have many of the primary and appended activities)

var query_text = 
"create or replace table " + INPUT.output_table + " as \n\n" +
"with rank_entities_to_find_suitable_example as ( \n" +
"    select  \n" +
"        " + INPUT.entity + ", \n" +
"        count(case when activity = '" + INPUT.get_activity + "' then ts end) as primary_activity_count, \n" +
"        count(case when activity = '" + APPEND_INPUT.activity + "' then ts end) as secondary_activity_count, \n" +
"        row_number() over (order by primary_activity_count desc) as primary_activity_rank, \n" +
"        row_number() over (order by secondary_activity_count desc) as secondary_activity_rank, \n" +
"        primary_activity_rank + secondary_activity_rank as primary_secondary_rank \n" +
"    from " + ACTIVITY_SCHEMA_LOCATION + "." + INPUT.entity + "_activities \n" +
"    where activity in ('" + INPUT.get_activity + "','" + APPEND_INPUT.activity + "')\n";


if ("filter" in INPUT != '') 
    query_text +=
    "       and " + INPUT.filter + " \n";

query_text += 
"   group by 1 \n" +
") \n\n";


query_text +=
", find_example_entity as ( \n" +
"    select  \n" +
"        "+ INPUT.entity + " as example_entity \n" +
"    from rank_entities_to_find_suitable_example \n" +
"    qualify row_number() over (order by primary_secondary_rank) = 1 \n" +
") \n\n";



// 2. GET ALL PRIMARY AND APPENDED ACTIVITIES FOR E.G. ENTITY

query_text +=
", all_activities as ( \n" +
"    select  \n" +
"        activity_id, \n" +
"        ts, \n" +
"        " + INPUT.entity + ", \n" +
"        activity, \n" +
"        activity_occurrence, \n" +
"        activity_repeated_at \n" +
"    from " + ACTIVITY_SCHEMA_LOCATION + "." + INPUT.entity + "_activities \n" +
"        join find_example_entity on " + INPUT.entity + " = example_entity \n" +
"    where activity in ('" + INPUT.get_activity + "','" + APPEND_INPUT.activity + "')\n";

if ("filter" in INPUT != '') 
    query_text +=
    "       and " + INPUT.filter + " \n";

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



//3. CALCULATE LINKS BETWEEN PRIMARY AND APPENDED ACTIVITIES
        
if (["first_ever", "last_ever", "first_before", "last_before", "last_after", "first_inbetween", "last_inbetween"].includes(APPEND_INPUT.append_relationship)) 
{

     query_text +=
    "select  \n" +
    "    all_activities." + INPUT.entity + ", \n" +
    "    all_activities.ts, \n" +
    "    case when all_activities.activity = '" + INPUT.get_activity + "' then all_activities.activity end as primary_activity, \n" +
    "    case when all_activities.activity = '" + INPUT.get_activity + "' then appended.activity_occurrence end as appended_to_secondary_activity_occurrence,     \n" +
    "    case when all_activities.activity = '" + APPEND_INPUT.activity + "' then all_activities.activity end as secondary_activity, \n" +
    "    case when all_activities.activity = '" + APPEND_INPUT.activity + "' then all_activities.activity_occurrence end as secondary_activity_occurrence \n" +
    "from all_activities \n";
    
    if (APPEND_INPUT.append_relationship == "first_ever" || APPEND_INPUT.append_relationship == "last_ever")
    {
        var append_filter = "appended.activity_occurrence = 1";
        if (APPEND_INPUT.append_relationship == "last_ever") append_filter = "appended.activity_repeated_at is null";

        query_text +=
        "       left join all_activities as appended \n" +
        "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
        "           and appended." + INPUT.entity + " = all_activities." + INPUT.entity + " \n" +
        "           and " + append_filter + "\n";

    }
    else if (APPEND_INPUT.append_relationship == "first_before") 
    {
        query_text += 
        "       left join all_activities as appended \n" +
        "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
        "           and appended." + INPUT.entity + " = all_activities." + INPUT.entity + " \n" +
        "           and appended.activity_occurrence = 1 \n" +
        "           and appended.ts < all_activities.ts \n";
    }
    else if (APPEND_INPUT.append_relationship == "last_before")
    {
        query_text += 
        "       left join all_activities as appended \n" +
        "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
        "           and appended." + INPUT.entity + " = all_activities." + INPUT.entity + " \n" +
        "           and appended.ts < all_activities.ts and all_activities.ts <= appended.activity_repeated_at \n";
    } 
    else if (APPEND_INPUT.append_relationship == "last_after")
    {
        query_text += 
        "       left join all_activities as appended \n" +
        "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
        "           and appended." + INPUT.entity + " = all_activities." + INPUT.entity + " \n" +
        "           and appended.activity_repeated_at is null \n" +
        "           and appended.ts > all_activities.ts \n";
    }
    else if (APPEND_INPUT.append_relationship == "first_inbetween" || APPEND_INPUT.append_relationship == "last_inbetween")
    {
        var sort_desc = ""
        if (APPEND_INPUT.append_relationship == "last_inbetween") sort_desc = "desc"

        query_text +=
        "       left join all_activities as appended \n" +
        "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
        "           and appended." + INPUT.entity + " = all_activities." + INPUT.entity + " \n" +
        "           and appended.ts between all_activities.ts \n" +
        "                             and coalesce(all_activities.activity_repeated_at, '9999-12-31') \n";

        query_text +=
        "qualify row_number() over (partition by all_activities.activity_id \n" +
        "                           order by appended.activity_occurrence " + sort_desc + ") = 1\n";
    }

}
else if (["aggregate_before", "aggregate_inbetween", "aggregate_after", "aggregate_all_ever", "aggregate"].includes(APPEND_INPUT.append_relationship))
{
    query_text +=
    "select  \n" +
    "    all_activities." + INPUT.entity + ", \n" +
    "    all_activities.ts, \n" +
    "    case when all_activities.activity = '" + INPUT.get_activity + "' then all_activities.activity end as primary_activity, \n" +
    "    listagg(case when all_activities.activity = '" + INPUT.get_activity + "' then appended.activity_occurrence end, ',')  \n" +
    "        within group (order by appended.activity_occurrence) as aggregated_secondary_activity_occurrences,     \n" +
    "    case when all_activities.activity = '" + APPEND_INPUT.activity + "' then all_activities.activity end as secondary_activity, \n" +
    "    case when all_activities.activity = '" + APPEND_INPUT.activity + "' then all_activities.activity_occurrence end as secondary_activity_occurrence   \n" +
    "from all_activities \n";

    if (APPEND_INPUT.append_relationship == "aggregate_before")
    {
        query_text +=
        "       left join all_activities as appended \n" +
        "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
        "           and appended." + INPUT.entity + " = all_activities." + INPUT.entity + " \n" +
        "           and appended.ts < all_activities.ts \n"; 
    } 
    else if (APPEND_INPUT.append_relationship == "aggregate_inbetween")
    {
        query_text +=
        "       left join all_activities as appended \n" +
        "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
        "           and appended." + INPUT.entity + " = all_activities." + INPUT.entity + " \n" +
        "           and appended.ts between all_activities.ts \n" +
        "                             and coalesce(all_activities.activity_repeated_at, '9999-12-31') \n";
    } 
    else if (APPEND_INPUT.append_relationship == "aggregate_after")
    {
        query_text +=
        "       left join all_activities as appended \n" +
        "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
        "           and appended." + INPUT.entity + " = all_activities." + INPUT.entity + " \n" +
        "           and appended.ts > all_activities.ts \n";
    } 
    else if (APPEND_INPUT.append_relationship == "aggregate_all_ever")
    {
        query_text +=
        "       left join all_activities as appended \n" +
        "           on  appended.activity = '" + APPEND_INPUT.activity + "' \n" +
        "           and appended." + INPUT.entity + " = all_activities." + INPUT.entity + " \n";
    } 

    query_text +=
    "group by 1,2,3,5,6 \n" ;
}

query_text +=
"order by all_activities.ts;";

var query_statement = snowflake.createStatement({sqlText:query_text});
var query_results = query_statement.execute();

return "-- " + INPUT.output_table +" created.\n-- Query:- \n" + query_text;
    
$$;;