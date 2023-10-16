



CREATE TABLE IF NOT EXISTS target_schema.etl_checkpoint(etl_last_run_date TIMESTAMP);

CREATE TABLE IF NOT EXISTS target_schema.etl_history(step_name TEXT, execution_start TIMESTAMP, execution_end TIMESTAMP);



