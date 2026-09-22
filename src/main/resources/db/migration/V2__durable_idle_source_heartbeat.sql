-- Explicit source-capture exception: only this small internal heartbeat table is CDC enabled.
-- Real committed heartbeat changes preserve durable scan progress during idle business periods.
IF OBJECT_ID(N'dbo.IQHR_CdcHeartbeat',N'U') IS NULL
BEGIN
    CREATE TABLE dbo.IQHR_CdcHeartbeat (
        singleton_id int NOT NULL CONSTRAINT PK_IQHR_CdcHeartbeat PRIMARY KEY,
        tick_sequence bigint NOT NULL,
        ticked_at datetime2(7) NOT NULL,
        CONSTRAINT CK_IQHR_CdcHeartbeat_Singleton CHECK(singleton_id=1)
    );
END;
IF COALESCE(COL_LENGTH('dbo.IQHR_CdcHeartbeat','singleton_id'),-2) <> 4
    OR COALESCE(COL_LENGTH('dbo.IQHR_CdcHeartbeat','tick_sequence'),-2) <> 8
    OR COL_LENGTH('dbo.IQHR_CdcHeartbeat','ticked_at') IS NULL
    THROW 51000, 'Incompatible IQHR_CdcHeartbeat: reviewed additive migration required', 1;
IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE database_id=DB_ID() AND is_cdc_enabled=1)
    THROW 51000, 'Database CDC must already be enabled before deploying CDC heartbeat', 1;
IF NOT EXISTS(SELECT 1 FROM cdc.change_tables WHERE source_object_id=OBJECT_ID(N'dbo.IQHR_CdcHeartbeat'))
    EXEC sys.sp_cdc_enable_table @source_schema=N'dbo',@source_name=N'IQHR_CdcHeartbeat',
         @capture_instance=N'dbo_IQHR_CdcHeartbeat',@role_name=NULL,@supports_net_changes=0;
