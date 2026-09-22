package com.iqhr.cdc.source;

import com.iqhr.cdc.CdcProperties.Tenant;
import com.iqhr.cdc.store.ContinuityException;
import java.sql.*;
import java.time.*;
import java.util.*;

/** Native SQL is confined to SQL Server CDC metadata, session locking and storage diagnostics. */
public final class SqlServerControl implements AutoCloseable {
    private final Connection connection;
    private final Tenant tenant;
    private final String resource;
    public SqlServerControl(Tenant tenant) throws SQLException {
        this.tenant=tenant; resource="IQHR.CDC.DatabasePipeline";
        connection=DriverManager.getConnection(tenant.jdbcUrl(),tenant.user(),tenant.password());
        connection.setAutoCommit(true);
        try (PreparedStatement statement=query("DECLARE @result int; EXEC @result=sys.sp_getapplock @Resource=?, @LockMode='Exclusive', @LockOwner='Session', @LockTimeout=0; SELECT @result")) {
            statement.setString(1,resource);
            try(ResultSet result=statement.executeQuery()) {
                if(!result.next()||result.getInt(1)<0)throw new ContinuityException("SOURCE_ALREADY_OWNED");
            }
        } catch (RuntimeException | SQLException e) { connection.close(); throw e; }
    }
    private PreparedStatement query(String sql) throws SQLException {
        PreparedStatement statement=connection.prepareStatement(sql); statement.setQueryTimeout(15); return statement;
    }
    public synchronized void assertOwnership() {
        try(PreparedStatement statement=query("SELECT APPLOCK_MODE('public',?,'Session')")) {
            statement.setString(1,resource);
            try(ResultSet result=statement.executeQuery()) {
                if(!result.next()||!"Exclusive".equals(result.getString(1)))throw new ContinuityException("SOURCE_LOCK_LOST");
            }
        } catch(SQLException e) { throw new ContinuityException("SOURCE_LOCK_UNAVAILABLE"); }
    }
    public synchronized Diagnostics inspect(String savedCommitLsn) throws SQLException {
        assertOwnership();
        Integer retention=null;
        try(PreparedStatement statement=query("EXEC sys.sp_cdc_help_jobs"); ResultSet result=statement.executeQuery()) {
            while(result.next())if("cleanup".equals(result.getString("job_type")))retention=result.getInt("retention");
        }
        if(retention==null)throw new ContinuityException("CDC_CLEANUP_JOB_MISSING");
        boolean captureRunning=false,jobStateUnavailable=false;
        try(PreparedStatement statement=query("SELECT TOP(1) a.start_execution_date,a.stop_execution_date FROM msdb.dbo.cdc_jobs c JOIN msdb.dbo.sysjobactivity a ON a.job_id=c.job_id WHERE c.database_id=DB_ID() AND c.job_type='capture' ORDER BY a.session_id DESC"); ResultSet result=statement.executeQuery()) {
            if(result.next())captureRunning=result.getTimestamp(1)!=null&&result.getTimestamp(2)==null;
        } catch(SQLException e) { jobStateUnavailable=true; }
        long headroom=retention.longValue()*60;
        byte[] saved=savedCommitLsn==null?null:parseLsn(savedCommitLsn);
        for(String table:tenant.capturedTables()) {
            boolean found=false,covered=saved==null;
            try(PreparedStatement statement=query("SELECT capture_instance,end_lsn FROM cdc.change_tables WHERE source_object_id=OBJECT_ID(?)")) {
                statement.setString(1,table);
                try(ResultSet result=statement.executeQuery()) {
                    while(result.next()) {
                        found=true;
                        byte[] end=result.getBytes(2);
                        try(PreparedStatement lsn=query("SELECT sys.fn_cdc_get_min_lsn(?),sys.fn_cdc_map_lsn_to_time(?),sys.fn_cdc_get_max_lsn()")) {
                            lsn.setString(1,result.getString(1)); lsn.setBytes(2,saved);
                            try(ResultSet positions=lsn.executeQuery()) {
                                positions.next(); byte[] minimum=positions.getBytes(1), maximum=positions.getBytes(3);
                                if(saved!=null&&covers(saved,minimum,end,maximum))covered=true;
                                Timestamp savedTime=positions.getTimestamp(2);
                                // CDC commit timestamps use SQL Server local time; compute age on the server below.
                                if(savedTime!=null) {
                                    try(PreparedStatement age=query("SELECT DATEDIFF_BIG(second,sys.fn_cdc_map_lsn_to_time(?),GETDATE())")) {
                                        age.setBytes(1,saved);
                                        try(ResultSet ages=age.executeQuery()) { ages.next(); headroom=Math.min(headroom,retention.longValue()*60-ages.getLong(1)); }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if(!found)throw new ContinuityException("SOURCE_TABLE_NOT_CDC_ENABLED");
            if(!covered)throw new ContinuityException("SOURCE_POSITION_UNAVAILABLE");
        }
        long freeBytes=Long.MAX_VALUE;
        boolean volumeUnavailable=false;
        try(PreparedStatement statement=query("SELECT MIN(v.available_bytes) FROM sys.database_files f CROSS APPLY sys.dm_os_volume_stats(DB_ID(),f.file_id) v"); ResultSet result=statement.executeQuery()) {
            if(result.next())freeBytes=result.getLong(1);
        } catch(SQLException e) { volumeUnavailable=true; }
        long cappedFreePages=Long.MAX_VALUE;
        try(PreparedStatement statement=query("SELECT MIN(CONVERT(bigint,max_size)-CONVERT(bigint,FILEPROPERTY(name,'SpaceUsed'))) FROM sys.database_files WHERE type=0 AND max_size>0"); ResultSet result=statement.executeQuery()) {
            if(result.next()) { long value=result.getLong(1); if(!result.wasNull())cappedFreePages=value; }
        }
        boolean recentErrors=false;
        try(PreparedStatement statement=query("SELECT TOP(1) 1 FROM sys.dm_cdc_errors WHERE entry_time > DATEADD(minute,-5,GETDATE())"); ResultSet result=statement.executeQuery()) { recentErrors=result.next(); }
        boolean stalled=false;
        try(PreparedStatement statement=query("SELECT TOP(1) DATEDIFF(second,end_time,GETDATE()) FROM sys.dm_cdc_log_scan_sessions WHERE session_id<>0 ORDER BY session_id DESC"); ResultSet result=statement.executeQuery()) {
            if(result.next())stalled=result.getInt(1)>300;
        }
        String warning=retention<tenant.minimumRetentionMinutes()?"CDC_RETENTION_BELOW_14_DAYS":
                !captureRunning&&!jobStateUnavailable?"CDC_CAPTURE_JOB_STOPPED":recentErrors?"CDC_CAPTURE_ERRORS":stalled?"CDC_SCAN_STALE":
                freeBytes<2L*1024*1024*1024||cappedFreePages<131072?"SOURCE_STORAGE_LOW":
                headroom<7L*86400?"CDC_RETENTION_HEADROOM_LOW":jobStateUnavailable?"CDC_JOB_STATE_METRICS_UNAVAILABLE":volumeUnavailable?"PHYSICAL_DISK_METRICS_UNAVAILABLE":null;
        return new Diagnostics(retention,headroom,warning);
    }
    public static byte[] parseLsn(String lsn) {
        if(lsn==null||!lsn.matches("[0-9a-fA-F]{8}:[0-9a-fA-F]{8}:[0-9a-fA-F]{4}"))throw new ContinuityException("INVALID_SOURCE_POSITION");
        return HexFormat.of().parseHex(lsn.replace(":",""));
    }
    private static int compare(byte[] first,byte[] second) { return Arrays.compareUnsigned(first,second); }
    public static boolean covers(byte[] saved,byte[] minimum,byte[] end,byte[] maximum) {
        return minimum!=null&&maximum!=null&&compare(saved,minimum)>=0&&compare(saved,maximum)<=0&&(end==null||compare(saved,end)<=0);
    }
    @Override public synchronized void close() throws SQLException { connection.close(); }
    public record Diagnostics(int retentionMinutes,long headroomSeconds,String warning) {}
}
