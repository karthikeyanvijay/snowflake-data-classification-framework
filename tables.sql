USE SCHEMA admin_db.data_classification;

CREATE OR REPLACE TABLE classification_metadata (
	object_name_regex STRING,
	exclude_from_classification STRING DEFAULT 'N', -- 'N' applies the Classification profile, Exclude takes priority
	classification_profile STRING,                  -- Not needed for exclude_from_classification = 'Y'
    include_regex_priority INTEGER                  -- If there are multiple regex that match, higher value gets selected
);