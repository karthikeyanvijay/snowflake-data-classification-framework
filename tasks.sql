USE SCHEMA admin_db.data_classification;
CREATE OR REPLACE TASK configure_data_classification_task
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = XSMALL
--WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON * 20 * * * UTC'
AS
CALL sp_configure_data_classification(
  default_classification_profile      => 'ADMIN_DB.DATA_CLASSIFICATION.STANDARD_CLASSIFICATION_PROFILE'
, execute_flag                          => 'Y'
, classification_configuration_table    => 'ADMIN_DB.DATA_CLASSIFICATION.CLASSIFICATION_METADATA'
, notification_config                   => 
        parse_json('{
            "EMAIL_INTEGRATION" :{
            "subject":"Automated Classification Configuration Report",
            "toAddress": ["firstname.lastname@company.com"]
        }
        }')
);