


CREATE TABLE IF NOT EXISTS target_schema.gbpjpy ( datetime TIMESTAMP, close TEXT );

CREATE INDEX IF NOT EXISTS idx ON target_schema.gbpjpy (datetime);

INSERT INTO target_schema.gbpjpy
    ( datetime, close)
SELECT 
    
	datetime, close
	FROM stg_schema.gbpjpy;





