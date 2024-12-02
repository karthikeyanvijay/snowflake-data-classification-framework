USE SCHEMA admin_db.data_classification;
CREATE OR REPLACE PROCEDURE sp_data_classification_account_configuration( default_classification_profile STRING
                                            , classification_configuration_table STRING DEFAULT ''
                                            , execute_flag STRING default 'N'
                                            , notification_config VARIANT default NULL
                                            )
COPY GRANTS
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'data_classification_account_configuration'
COMMENT = 'Stored Procedure to setup automatic data classification on schemas'
EXECUTE AS CALLER
AS
$$
import snowflake
import pandas as pd
from snowflake.snowpark.types import *
import json
import datetime
import pytz
import sys


def get_show_schemas_query_id(session):
    session.sql("SHOW SCHEMAS IN ACCOUNT").collect()
    show_schemas_query_id = session.sql("SELECT LAST_QUERY_ID()").collect()[0][0]
    return show_schemas_query_id


def get_show_databases_query_id(session):
    session.sql("SHOW DATABASES IN ACCOUNT").collect()
    show_databases_query_id = session.sql("SELECT LAST_QUERY_ID()").collect()[0][0]
    return show_databases_query_id


def apply_query_tag(session, execute_flag):
    timezone = pytz.timezone("UTC")
    current_time = datetime.datetime.now(timezone).strftime("%Y-%m-%dT%H:%M:%SZ")
    python_version = str(sys.version_info.major) + '.' + str(sys.version_info.minor)
    query_tag_json = json.dumps(
        {
            "process":              "sp_data_classification_account_configuration",
            "process_version":      "2024-11-25",
            "start_time":           current_time,
            "execute_flag":         execute_flag,
            "python_version":       python_version
        }
    )
    alter_session_command = f"ALTER SESSION SET QUERY_TAG = '{query_tag_json}'"
    session.query_tag = query_tag_json


def send_email(session, data_df, notification_config):
    html_body = data_df.to_html(index=False).replace("'", "&#39;")
    has_failed = (
        data_df["alter_statement_execution_result"]
        .str.lower()
        .str.contains("failed")
        .any()
    )
    org_acc_name = session.sql(
        "SELECT CONCAT(current_organization_name(), '-', current_account_name())"
    ).collect()[0][0]

    integration_name = list(notification_config.keys())[0]
    original_subject = notification_config[integration_name]["subject"]

    updated_subject = f"{org_acc_name} - {original_subject}"
    if has_failed:
        updated_subject += " - Failed to set some configuration"

    notification_config[integration_name]["subject"] = updated_subject

    updated_config_json = json.dumps(notification_config)
    message_json = (
        f"<html><body><p>Recent Configuration changes</p>{html_body}</body></html>"
    )

    send_email_command = f"""
    CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
        SNOWFLAKE.NOTIFICATION.TEXT_HTML('{message_json}'),
        parse_json('{updated_config_json}')
    );
    """
    session.sql(send_email_command).collect()


def get_changes_required_df(
    session,
    classification_configuration_table,
    default_classification_profile,
    show_databases_query_id,
    show_schemas_query_id,
):

    # Default so that we can handle a scenario where the configuration table is not present
    configuration_query = """
            SELECT null as object_name_regex
                    , null as exclude_from_classification
                    , null as classification_profile 
                    , null AS configuration_classification_profile
                    , null as include_regex_priority
            WHERE 1 = 2
    """
    if classification_configuration_table != "":
        configuration_query = f"""
            SELECT object_name_regex
                    , exclude_from_classification
                    , classification_profile AS configuration_classification_profile
                    , include_regex_priority
            FROM {classification_configuration_table}
    """

    query = f"""
        WITH all_databases AS
        (
            SELECT "name" as database_name
                    , "kind" AS kind
            FROM TABLE(RESULT_SCAN('{show_databases_query_id}'))
            WHERE kind = 'STANDARD'
        )
        , all_schemas AS 
        (
            SELECT concat("database_name", '.',"name") AS full_schema_name
                    , concat("classification_profile_database",'.'
                            ,"classification_profile_schema",'.'
                            ,"classification_profile") AS current_classification_profile 
            FROM TABLE(RESULT_SCAN('{show_schemas_query_id}'))
            WHERE TRUE
                  AND "name" NOT IN ('INFORMATION_SCHEMA')
                  AND "database_name" IN ( SELECT database_name FROM all_databases )
        ),
        configuration_metadata_schema AS 
        (
            SELECT * FROM 
            TABLE(TO_QUERY('{configuration_query}'))
        ),
        exclude_regex AS 
        (
            SELECT listagg(object_name_regex,'|') AS object_name_regex  
            FROM configuration_metadata_schema 
            WHERE exclude_from_classification = 'Y'
        ),
        schemas_for_classification AS 
        (
            SELECT s.full_schema_name
                    ,s.current_classification_profile
                    ,config.configuration_classification_profile
                    -- If the schema is not excluded, then a default profile is assigned
                     ,coalesce(config.configuration_classification_profile, '{default_classification_profile}') AS required_classification_profile
            FROM all_schemas s
            LEFT OUTER JOIN 
            (
                SELECT configuration_classification_profile
                    , include_regex_priority
                    , listagg(object_name_regex,'|') AS object_name_regex  
                FROM configuration_metadata_schema 
                WHERE exclude_from_classification = 'N'
                GROUP BY ALL
            ) config
            ON REGEXP_LIKE(s.full_schema_name,config.object_name_regex)
            WHERE TRUE 
                -- Exclude takes preference
                AND NOT REGEXP_LIKE(full_schema_name,(SELECT object_name_regex FROM exclude_regex))
                AND NVL(s.current_classification_profile,'') != required_classification_profile
            QUALIFY ROW_NUMBER() OVER (PARTITION BY s.full_schema_name 
                                        ORDER BY config.include_regex_priority DESC) = 1
        )
        , schemas_remove_classification AS 
        (
            --Get schemas to remove classification
            SELECT full_schema_name
                ,  current_classification_profile
                ,  null AS configuration_classification_profile
                ,  null AS required_classification_profile
            FROM all_schemas s
            WHERE TRUE
                AND REGEXP_LIKE(full_schema_name,(SELECT object_name_regex FROM exclude_regex))
                AND current_classification_profile IS NOT NULL
        )
        SELECT * FROM
        (
            SELECT * FROM schemas_for_classification 
            UNION ALL
            SELECT * FROM schemas_remove_classification
        )
        ORDER BY configuration_classification_profile NULLS LAST, 
                full_schema_name,
                required_classification_profile
        ;
    """
    changes_required_df = session.sql(query).to_pandas()
    return changes_required_df


def apply_classification_changes(session, changes_required_df):
    results = []
    success_rows = []
    error_rows = []

    for row in changes_required_df.itertuples():
        schema_name = row.FULL_SCHEMA_NAME
        required_classification_profile = row.REQUIRED_CLASSIFICATION_PROFILE
        configuration_classification_profile = row.CONFIGURATION_CLASSIFICATION_PROFILE
        current_classification_profile = row.CURRENT_CLASSIFICATION_PROFILE

        if required_classification_profile is None:
            alter_stmt = f"ALTER SCHEMA {schema_name} UNSET CLASSIFICATION_PROFILE;"
        else:
            alter_stmt = f"ALTER SCHEMA {schema_name} SET CLASSIFICATION_PROFILE = '{required_classification_profile}';"

        try:
            session.sql(alter_stmt).collect()
            success_rows.append(
                {
                    "full_schema_name": schema_name,
                    "current_classification_profile": current_classification_profile,
                    "configuration_classification_profile": configuration_classification_profile,
                    "required_configuration_profile": required_classification_profile,
                    "alter_statement_execution_result": "SUCCESS",
                    "alter_statement": alter_stmt,
                }
            )
        except Exception as e:
            error_rows.append(
                {
                    "full_schema_name": schema_name,
                    "current_classification_profile": current_classification_profile,
                    "configuration_classification_profile": configuration_classification_profile,
                    "required_configuration_profile": required_classification_profile,
                    "alter_statement_execution_result": f"FAILED: {str(e)}",
                    "alter_statement": alter_stmt,
                }
            )

    result_df = pd.concat(
        [pd.DataFrame(success_rows), pd.DataFrame(error_rows)], ignore_index=True
    )
    return result_df


def data_classification_account_configuration(
    session,
    default_classification_profile: str,
    classification_configuration_table: str,
    execute_flag: str,
    notification_config: dict,
) -> snowflake.snowpark.DataFrame:

    apply_query_tag(session, execute_flag)

    out_schema = StructType(
        [
            StructField("full_schema_name", StringType()),
            StructField("current_classification_profile", StringType()),
            StructField("configuration_classification_profile", StringType()),
            StructField("required_configuration_profile", StringType()),
            StructField("alter_statement_execution_result", StringType()),
            StructField("alter_statement", StringType()),
            
        ]
    )

    # Get query IDs
    show_databases_query_id = get_show_databases_query_id(session)
    show_schemas_query_id = get_show_schemas_query_id(session)

    # Get changes required dataframe
    changes_required_df = get_changes_required_df(
        session,
        classification_configuration_table,
        default_classification_profile,
        show_databases_query_id,
        show_schemas_query_id,
    )
    
    return_df = session.create_dataframe([], schema=out_schema)
    if execute_flag == "N":
        return_df = session.create_dataframe(changes_required_df)
    else:
        result_df = apply_classification_changes(session, changes_required_df)
        if len(result_df.index) == 0:
            return_df = session.create_dataframe([], schema=out_schema)
        else:
            if type(notification_config).__name__ != "sqlNullWrapper":
                send_email(session, result_df, notification_config)
            return_df = session.create_dataframe(result_df)
    
    session.query_tag = None
    return return_df

$$;