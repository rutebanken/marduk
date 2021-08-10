DROP TABLE IF EXISTS CAMEL_FILEPROCESSED;

CREATE TABLE IF NOT EXISTS CAMEL_UNIQUE_FILENAME_AND_DIGEST (processorName VARCHAR(255), digest VARCHAR(255), fileName varchar(255),createdAt TIMESTAMP);

alter table camel_unique_filename_and_digest
    add constraint camel_unique_filename_and_digest_pk
        primary key (processorname, digest, filename);

--CREATE UNIQUE INDEX IF NOT EXISTS  CAMEL_UNIQUE_FILENAME_AND_DIGEST_UNIQUE_DIGEST_IDX ON CAMEL_UNIQUE_FILENAME_AND_DIGEST (processorName,digest); --CREATE INDEX IF NOT EXISTS NOT SUPPORTED in 9.4, requires postgres 9.5..
--CREATE UNIQUE INDEX IF NOT EXISTS  CAMEL_UNIQUE_FILENAME_AND_DIGEST_UNIQUE_NAME_IDX ON CAMEL_UNIQUE_FILENAME_AND_DIGEST (processorName,fileName);