USE SCHEMA admin_db.data_classification;

CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE
  standard_classification_profile(
    {
      'maximum_classification_validity_days': 180,
      'minimum_object_age_for_classification_days': 7,
      'auto_tag': true
    });
    
CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE
  monthly_classification_profile(
    {
      'maximum_classification_validity_days': 30,
      'minimum_object_age_for_classification_days': 7,
      'auto_tag': true
    });

CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE
  sensitive_classification_profile(
    {
      'maximum_classification_validity_days': 7,
      'minimum_object_age_for_classification_days': 1,
      'auto_tag': true
    });