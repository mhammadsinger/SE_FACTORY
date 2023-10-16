



CREATE TABLE IF NOT EXISTS target_schema.eurusd ( datetime TIMESTAMP, close TEXT );

CREATE INDEX IF NOT EXISTS idx ON target_schema.eurusd (datetime);

INSERT INTO target_schema.eurusd
    ( datetime, close)
SELECT 
    
	datetime, close
	FROM stg_schema.eurusd;

