-- Read-only SQL Server operator diagnostics. Select the tenant database first.
-- Never reset offsets/recreate objects to repair a failed source.
SELECT DB_NAME() AS database_name,is_cdc_enabled FROM sys.databases WHERE database_id=DB_ID();
EXEC sys.sp_cdc_help_jobs;
SELECT capture_instance,OBJECT_SCHEMA_NAME(source_object_id) AS schema_name,
       OBJECT_NAME(source_object_id) AS table_name,start_lsn,end_lsn,
       sys.fn_cdc_get_min_lsn(capture_instance) AS minimum_retained_lsn
FROM cdc.change_tables;
SELECT sys.fn_cdc_get_max_lsn() AS maximum_captured_lsn;
SELECT source_id,tenant_id,state,message,updated_at,last_event_at,last_commit_lsn,
       retention_minutes,retention_headroom_seconds,capture_state,consumer_state
FROM dbo.IQHR_CdcStatus;
SELECT source_id,tenant_id,incarnation,created_at,fence FROM dbo.IQHR_CdcActivation;
SELECT COUNT_BIG(*) AS durable_offset_partitions FROM dbo.IQHR_CdcOffset;
SELECT COUNT_BIG(*) AS schema_history_parts FROM dbo.IQHR_CdcSchemaHistory;
SELECT name,size*8.0/1024 AS allocated_mb,FILEPROPERTY(name,'SpaceUsed')*8.0/1024 AS used_mb,
       max_size AS maximum_pages,growth,is_percent_growth FROM sys.database_files;
SELECT TOP(10) session_id,start_time,end_time,error_count,empty_scan_count
FROM sys.dm_cdc_log_scan_sessions ORDER BY session_id DESC;
-- These require server-level diagnostic permission; the service reports DEGRADED if unavailable.
-- SELECT f.name,v.available_bytes FROM sys.database_files f
-- CROSS APPLY sys.dm_os_volume_stats(DB_ID(),f.file_id) v;
-- Administrator-reviewed one-time retention change, if required:
-- EXEC sys.sp_cdc_change_job @job_type=N'cleanup', @retention=20160;
