




CREATE TABLE IF NOT EXISTS target_schema.gbpusd ( datetime TIMESTAMP, close TEXT );

CREATE INDEX IF NOT EXISTS idx ON target_schema.gbpusd (datetime);

INSERT INTO target_schema.gbpusd
    ( datetime, close)
SELECT 
    
	datetime, close
	FROM stg_schema.gbpusd;

