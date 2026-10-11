-- Online schema evolution and per-table isolation for the table registry.
-- Additive only: each column is added when absent; an existing column with another shape stops the migration.
IF COL_LENGTH(N'dbo.IQHR_CdcTable',N'schema_state') IS NULL ALTER TABLE dbo.IQHR_CdcTable ADD schema_state nvarchar(32) NULL;
IF COL_LENGTH(N'dbo.IQHR_CdcTable',N'schema_detail') IS NULL ALTER TABLE dbo.IQHR_CdcTable ADD schema_detail nvarchar(2000) NULL;
IF COL_LENGTH(N'dbo.IQHR_CdcTable',N'capture_instances') IS NULL ALTER TABLE dbo.IQHR_CdcTable ADD capture_instances nvarchar(1000) NULL;
IF COL_LENGTH(N'dbo.IQHR_CdcTable',N'refresh_requested_at') IS NULL ALTER TABLE dbo.IQHR_CdcTable ADD refresh_requested_at datetime2(7) NULL;
IF COL_LENGTH(N'dbo.IQHR_CdcTable',N'refresh_requested_by') IS NULL ALTER TABLE dbo.IQHR_CdcTable ADD refresh_requested_by nvarchar(256) NULL;
IF COL_LENGTH(N'dbo.IQHR_CdcTable',N'refresh_started_at') IS NULL ALTER TABLE dbo.IQHR_CdcTable ADD refresh_started_at datetime2(7) NULL;
IF COL_LENGTH(N'dbo.IQHR_CdcTable',N'refresh_detail') IS NULL ALTER TABLE dbo.IQHR_CdcTable ADD refresh_detail nvarchar(2000) NULL;
IF COL_LENGTH(N'dbo.IQHR_CdcTable',N'refreshed_at') IS NULL ALTER TABLE dbo.IQHR_CdcTable ADD refreshed_at datetime2(7) NULL;
IF COL_LENGTH(N'dbo.IQHR_CdcTable',N'refresh_error') IS NULL ALTER TABLE dbo.IQHR_CdcTable ADD refresh_error nvarchar(200) NULL;
IF COL_LENGTH(N'dbo.IQHR_CdcTable',N'suspended_at') IS NULL ALTER TABLE dbo.IQHR_CdcTable ADD suspended_at datetime2(7) NULL;
IF COL_LENGTH(N'dbo.IQHR_CdcTable',N'suspension_reason') IS NULL ALTER TABLE dbo.IQHR_CdcTable ADD suspension_reason nvarchar(64) NULL;
IF COL_LENGTH(N'dbo.IQHR_CdcTable',N'suspended_lsn') IS NULL ALTER TABLE dbo.IQHR_CdcTable ADD suspended_lsn nvarchar(32) NULL;

IF EXISTS (
    SELECT 1 FROM (VALUES
        ('schema_state','nvarchar',64),('schema_detail','nvarchar',4000),('capture_instances','nvarchar',2000),
        ('refresh_requested_at','datetime2',8),('refresh_requested_by','nvarchar',512),('refresh_started_at','datetime2',8),
        ('refresh_detail','nvarchar',4000),('refreshed_at','datetime2',8),('refresh_error','nvarchar',400),
        ('suspended_at','datetime2',8),('suspension_reason','nvarchar',128),('suspended_lsn','nvarchar',64)
    ) expected(column_name,type_name,max_length)
    LEFT JOIN sys.columns c ON c.object_id=OBJECT_ID(N'dbo.IQHR_CdcTable') AND c.name=expected.column_name
    LEFT JOIN sys.types t ON t.user_type_id=c.user_type_id
    WHERE c.column_id IS NULL OR t.name<>expected.type_name OR c.max_length<>expected.max_length OR c.is_nullable<>1
        OR (t.name='datetime2' AND c.scale<>7)
)
    THROW 51000,'Incompatible IQHR_CdcTable schema evolution columns; explicit reviewed migration required',1;
