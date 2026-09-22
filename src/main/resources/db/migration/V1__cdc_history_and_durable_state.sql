-- Service-owned additive schema only. No CDC enablement and no business table writes.
IF OBJECT_ID(N'dbo.IQHR_CdcEvent', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.IQHR_CdcEvent (
        event_id varchar(64) NOT NULL PRIMARY KEY,
        tenant_id nvarchar(64) NOT NULL, source_id nvarchar(100) NOT NULL,
        schema_name nvarchar(128) NOT NULL, table_name nvarchar(128) NOT NULL,
        operation varchar(16) NOT NULL, occurred_at datetime2(7) NOT NULL, captured_at datetime2(7) NOT NULL,
        record_key nvarchar(max) NOT NULL, before_json nvarchar(max) NULL, after_json nvarchar(max) NULL,
        changed_properties nvarchar(max) NOT NULL, commit_lsn varchar(32) NULL, change_lsn varchar(32) NULL,
        event_serial_no bigint NULL
    );
END;
IF COALESCE(COL_LENGTH('dbo.IQHR_CdcEvent','event_id'),-2) <> 64 OR COALESCE(COL_LENGTH('dbo.IQHR_CdcEvent','record_key'),-2) <> -1
    OR COL_LENGTH('dbo.IQHR_CdcEvent','changed_properties') IS NULL
    THROW 51000, 'Incompatible IQHR_CdcEvent: reviewed additive migration required', 1;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('dbo.IQHR_CdcEvent') AND name='IX_IQHR_CdcEvent_Time')
    CREATE INDEX IX_IQHR_CdcEvent_Time ON dbo.IQHR_CdcEvent(occurred_at DESC,event_id DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('dbo.IQHR_CdcEvent') AND name='IX_IQHR_CdcEvent_TableTime')
    CREATE INDEX IX_IQHR_CdcEvent_TableTime ON dbo.IQHR_CdcEvent(schema_name,table_name,occurred_at DESC,event_id DESC);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('dbo.IQHR_CdcEvent') AND name='IX_IQHR_CdcEvent_OperationTime')
    CREATE INDEX IX_IQHR_CdcEvent_OperationTime ON dbo.IQHR_CdcEvent(operation,occurred_at DESC,event_id DESC);

IF OBJECT_ID(N'dbo.IQHR_CdcStatus', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.IQHR_CdcStatus (
        source_id nvarchar(100) NOT NULL PRIMARY KEY, tenant_id nvarchar(64) NOT NULL,
        state varchar(32) NOT NULL, message nvarchar(1000) NULL, updated_at datetime2(7) NOT NULL,
        last_event_at datetime2(7) NULL, last_commit_lsn varchar(32) NULL,
        retention_minutes int NULL, retention_headroom_seconds bigint NULL,
        capture_state varchar(32) NOT NULL, consumer_state varchar(32) NOT NULL
    );
END;
IF COL_LENGTH('dbo.IQHR_CdcStatus','capture_state') IS NULL
    ALTER TABLE dbo.IQHR_CdcStatus ADD capture_state varchar(32) NOT NULL CONSTRAINT DF_CdcCaptureState DEFAULT 'STARTING';
IF COL_LENGTH('dbo.IQHR_CdcStatus','consumer_state') IS NULL
    ALTER TABLE dbo.IQHR_CdcStatus ADD consumer_state varchar(32) NOT NULL CONSTRAINT DF_CdcConsumerState DEFAULT 'STARTING';
IF COL_LENGTH('dbo.IQHR_CdcStatus','retention_headroom_seconds') IS NULL
    THROW 51000, 'Incompatible IQHR_CdcStatus: reviewed additive migration required', 1;

IF OBJECT_ID(N'dbo.IQHR_CdcActivation', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.IQHR_CdcActivation (
        source_id nvarchar(100) NOT NULL PRIMARY KEY, tenant_id nvarchar(64) NOT NULL,
        incarnation varchar(36) NOT NULL, configuration_hash varchar(64) NOT NULL,
        created_at datetime2(7) NOT NULL, fence bigint NOT NULL
    );
END;
IF COALESCE(COL_LENGTH('dbo.IQHR_CdcActivation','fence'),-2) <> 8 OR COALESCE(COL_LENGTH('dbo.IQHR_CdcActivation','incarnation'),-2) <> 36
    THROW 51000, 'Incompatible IQHR_CdcActivation: reviewed additive migration required', 1;

IF OBJECT_ID(N'dbo.IQHR_CdcOffset', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.IQHR_CdcOffset (
        id varchar(36) NOT NULL PRIMARY KEY, offset_key nvarchar(max) NULL, offset_val nvarchar(max) NULL,
        record_insert_ts datetime2(7) NOT NULL, record_insert_seq int NOT NULL
    );
END;
IF COALESCE(COL_LENGTH('dbo.IQHR_CdcOffset','offset_val'),-2) <> -1
    THROW 51000, 'Incompatible IQHR_CdcOffset: reviewed additive migration required', 1;

IF OBJECT_ID(N'dbo.IQHR_CdcSchemaHistory', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.IQHR_CdcSchemaHistory (
        id varchar(36) NOT NULL, history_data nvarchar(max) NOT NULL, history_data_seq int NOT NULL,
        record_insert_ts datetime2(7) NOT NULL, record_insert_seq int NOT NULL,
        storage_sequence bigint IDENTITY(1,1) NOT NULL,
        CONSTRAINT PK_IQHR_CdcSchemaHistory PRIMARY KEY (id,history_data_seq),
        CONSTRAINT UQ_IQHR_CdcSchemaHistory_Sequence UNIQUE (storage_sequence)
    );
END;
IF COL_LENGTH('dbo.IQHR_CdcSchemaHistory','storage_sequence') IS NULL
    THROW 51000, 'Incompatible IQHR_CdcSchemaHistory: reviewed additive migration required', 1;
