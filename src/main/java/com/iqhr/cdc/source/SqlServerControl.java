package com.iqhr.cdc.source;

import com.iqhr.cdc.CdcProperties.Tenant;
import com.iqhr.cdc.store.ContinuityException;
import com.iqhr.cdc.store.TenantStore;
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
        return inspect(savedCommitLsn,tenant.capturedTables());
    }
    public synchronized Diagnostics inspect(String savedCommitLsn,Collection<String> capturedTables) throws SQLException {
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
        for(String table:capturedTables) {
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
    /** SQL Server-owned catalog metadata cannot be represented by application table entities. */
    public synchronized List<TableObservation> observeTables() throws SQLException {
        assertOwnership();
        Map<String,List<Capture>> grouped=new TreeMap<>();
        Map<String,List<String>> descriptors=new HashMap<>();
        Map<String,Integer> sources=new HashMap<>();
        Map<String,String> names=new HashMap<>();
        try(PreparedStatement statement=query("SELECT s.name,t.name,ct.capture_instance,ct.object_id,ct.source_object_id,ct.create_date,ct.start_lsn,ct.role_name,ct.filegroup_name,ct.supports_net_changes,ct.index_name FROM cdc.change_tables ct JOIN sys.tables t ON t.object_id=ct.source_object_id JOIN sys.schemas s ON s.schema_id=t.schema_id WHERE t.is_ms_shipped=0");ResultSet rows=statement.executeQuery()) {
            while(rows.next()) {
                String schema=rows.getString(1),table=rows.getString(2);
                if(table.toLowerCase(Locale.ROOT).contains("iqhr_cdc")||"cdc".equalsIgnoreCase(schema))continue;
                String name=schema+"."+table;
                int captureId=rows.getInt(4),sourceId=rows.getInt(5);
                LocalDateTime created=rows.getTimestamp(6).toLocalDateTime();
                descriptors.computeIfAbsent(name,ignored -> new ArrayList<>()).add(sourceId+"|"+captureId+"|"+rows.getString(3)+"|"+created
                        +"|"+columnShape(sourceId)+"|"+columnShape(captureId)+"|"+keyShape(sourceId));
                // start_lsn is the instance's low watermark: set at creation and advanced by CDC cleanup.
                byte[] start=rows.getBytes(7);
                grouped.computeIfAbsent(name,ignored -> new ArrayList<>()).add(new Capture(rows.getString(3),created,start,
                        hasCapturedKey(sourceId,captureId),rows.getString(8),rows.getString(9),
                        rows.getBoolean(10),rows.getString(11),columns(captureId,true)));
                sources.put(name,sourceId);names.put(name,schema);
            }
        }
        List<TableObservation> result=new ArrayList<>();
        for(var item:grouped.entrySet()) {
            String name=item.getKey(),schema=names.get(name),table=name.substring(schema.length()+1);
            List<Capture> captures=item.getValue().stream().sorted(Comparator.comparing(Capture::createdAt).thenComparing(Capture::name)).toList();
            boolean supported=name.matches("[A-Za-z_][A-Za-z0-9_]*\\.[A-Za-z_][A-Za-z0-9_]*");
            // During a capture refresh the new instance may still be initializing; one ready keyed instance keeps the table readable.
            boolean ready=captures.stream().anyMatch(Capture::ready);
            boolean keyed=captures.stream().filter(Capture::ready).allMatch(Capture::keyed)&&captures.stream().anyMatch(Capture::keyed);
            String fingerprint=TenantStore.sha256(String.join("\n",descriptors.get(name).stream().sorted().toList()));
            result.add(new TableObservation(schema,table,fingerprint,supported&&ready&&keyed,
                    !supported?"TABLE_IDENTIFIER_UNSUPPORTED":!keyed?"CAPTURED_PRIMARY_KEY_REQUIRED":!ready?"CDC_CAPTURE_INITIALIZING":null,
                    captures.stream().filter(Capture::ready).map(Capture::start).toList(),captures,columns(sources.get(name),false)));
        }
        return List.copyOf(result);
    }
    private List<Column> columns(int objectId,boolean changeTable) throws SQLException {
        List<Column> result=new ArrayList<>();
        // Computed columns are never captured by SQL Server CDC, so they are not part of the capturable shape.
        try(PreparedStatement statement=query("SELECT name,TYPE_NAME(system_type_id),max_length,precision,scale,is_nullable,is_computed FROM sys.columns WHERE object_id=? ORDER BY column_id")) {
            statement.setInt(1,objectId);
            try(ResultSet rows=statement.executeQuery()) {
                while(rows.next()) {
                    String name=rows.getString(1);
                    if((changeTable&&name.startsWith("__$"))||rows.getBoolean(7))continue;
                    String type=rows.getString(2).toLowerCase(Locale.ROOT);
                    int length=rows.getInt(3);
                    if(length>0&&(type.equals("nvarchar")||type.equals("nchar")))length/=2;
                    result.add(new Column(name,type,length,rows.getInt(4),rows.getInt(5),rows.getBoolean(6)));
                }
            }
        }
        return List.copyOf(result);
    }
    /** Creates an additional capture instance; Debezium switches to it at its start LSN without a gap. */
    public synchronized void createCaptureInstance(String schema,String table,Capture template,String captureInstance) throws SQLException {
        assertOwnership();
        try(PreparedStatement statement=query("EXEC sys.sp_cdc_enable_table @source_schema=?,@source_name=?,@role_name=?,@capture_instance=?,@supports_net_changes=?,@index_name=?,@filegroup_name=?")) {
            statement.setQueryTimeout(120);
            statement.setString(1,schema);statement.setString(2,table);statement.setString(3,template.role());
            statement.setString(4,captureInstance);statement.setBoolean(5,template.netChanges());
            statement.setString(6,template.index());statement.setString(7,template.filegroup());
            statement.execute();
        }
    }
    /** Retires a superseded capture instance once the saved position is past its replacement's start. */
    public synchronized void dropCaptureInstance(String schema,String table,String captureInstance) throws SQLException {
        assertOwnership();
        try(PreparedStatement statement=query("EXEC sys.sp_cdc_disable_table @source_schema=?,@source_name=?,@capture_instance=?")) {
            statement.setQueryTimeout(120);
            statement.setString(1,schema);statement.setString(2,table);statement.setString(3,captureInstance);
            statement.execute();
        }
    }
    private String columnShape(int objectId) throws SQLException {
        StringBuilder shape=new StringBuilder();
        try(PreparedStatement statement=query("SELECT column_id,name,system_type_id,user_type_id,max_length,precision,scale,is_nullable FROM sys.columns WHERE object_id=? ORDER BY column_id")) {
            statement.setInt(1,objectId);
            try(ResultSet rows=statement.executeQuery()) {while(rows.next())for(int column=1;column<=8;column++)shape.append(rows.getString(column)).append('|');}
        }
        return shape.toString();
    }
    private boolean hasCapturedKey(int sourceId,int captureId) throws SQLException {
        try(PreparedStatement statement=query("SELECT COUNT(*),SUM(CASE WHEN cc.column_name IS NULL THEN 1 ELSE 0 END) FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id AND ic.key_ordinal>0 JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id LEFT JOIN cdc.captured_columns cc ON cc.object_id=? AND cc.column_name=c.name WHERE i.object_id=? AND i.is_primary_key=1")) {
            statement.setInt(1,captureId);statement.setInt(2,sourceId);
            try(ResultSet rows=statement.executeQuery()) {return rows.next()&&rows.getInt(1)>0&&rows.getInt(2)==0;}
        }
    }
    private String keyShape(int sourceId) throws SQLException {
        StringBuilder shape=new StringBuilder();
        try(PreparedStatement statement=query("SELECT ic.key_ordinal,c.name FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id WHERE i.object_id=? AND i.is_primary_key=1 AND ic.key_ordinal>0 ORDER BY ic.key_ordinal")) {
            statement.setInt(1,sourceId);
            try(ResultSet rows=statement.executeQuery()) {while(rows.next())shape.append(rows.getInt(1)).append(':').append(rows.getString(2)).append('|');}
        }
        return shape.toString();
    }
    /** A captured internal tick proves all source commits before this service-side activation request were scanned. */
    public synchronized String onboardingBoundary(long heartbeatTick) throws SQLException {
        assertOwnership();
        try(PreparedStatement statement=query("SELECT TOP(1) [__$start_lsn],sys.fn_cdc_get_max_lsn() FROM cdc.dbo_IQHR_CdcHeartbeat_CT WHERE singleton_id=1 AND tick_sequence=? AND [__$operation] IN (2,4) ORDER BY [__$start_lsn] DESC")) {
            statement.setLong(1,heartbeatTick);
            try(ResultSet rows=statement.executeQuery()) {
                if(!rows.next())return null;
                byte[] barrier=rows.getBytes(1),maximum=rows.getBytes(2);
                if(!validMinimum(barrier)||!validMinimum(maximum)||compare(maximum,barrier)<0)return null;
                String hex=HexFormat.of().formatHex(maximum);
                return hex.substring(0,8)+":"+hex.substring(8,16)+":"+hex.substring(16);
            }
        }
    }
    /** Expansion must not change the connector's skip counter at its exact saved position. */
    public synchronized boolean boundaryHasNoNewRows(Collection<String> tables,String commitLsn,String changeLsn) throws SQLException {
        assertOwnership();
        if("NULL".equals(changeLsn))return true;
        for(String table:tables) {
            try(PreparedStatement captures=query("SELECT capture_instance FROM cdc.change_tables WHERE source_object_id=OBJECT_ID(?)")) {
                captures.setString(1,table);
                try(ResultSet instances=captures.executeQuery()) {
                    boolean found=false;
                    while(instances.next()) {
                        found=true;
                        // Identifier comes from SQL Server's CDC catalog, quoted as one identifier; all values are bound.
                        String changeTable="["+(instances.getString(1)+"_CT").replace("]","]]")+"]";
                        try(PreparedStatement statement=query("SELECT TOP(1) 1 FROM cdc."+changeTable+" WHERE [__$start_lsn]=? AND [__$seqval]=?")) {
                            statement.setBytes(1,parseLsn(commitLsn));statement.setBytes(2,parseLsn(changeLsn));
                            try(ResultSet rows=statement.executeQuery()) {if(rows.next())return false;}
                        }
                    }
                    if(!found)return false;
                }
            }
        }
        return true;
    }
    static boolean validMinimum(byte[] value) {return value!=null&&value.length==10&&!Arrays.equals(value,new byte[10]);}
    public record Column(String name,String type,int length,int precision,int scale,boolean nullable) {}
    public record Capture(String name,LocalDateTime createdAt,byte[] start,boolean keyed,String role,String filegroup,
                          boolean netChanges,String index,List<Column> columns) {
        public boolean ready() {return validMinimum(start);}
    }
    public record TableObservation(String schema,String table,String fingerprint,boolean eligible,String diagnostic,List<byte[]> minimums,
                                   List<Capture> captures,List<Column> sourceColumns) {
        public TableObservation(String schema,String table,String fingerprint,boolean eligible,String diagnostic,List<byte[]> minimums) {
            this(schema,table,fingerprint,eligible,diagnostic,minimums,List.of(),List.of());
        }
        public String qualifiedName() {return schema+"."+table;}
        public boolean coversStart(String saved) {
            byte[] lsn=parseLsn(saved);
            return eligible&&minimums.stream().anyMatch(min -> validMinimum(min)&&compare(lsn,min)>=0);
        }
    }
    public static byte[] parseLsn(String lsn) {
        if(lsn==null||!lsn.matches("[0-9a-fA-F]{8}:[0-9a-fA-F]{8}:[0-9a-fA-F]{4}"))throw new ContinuityException("INVALID_SOURCE_POSITION");
        return HexFormat.of().parseHex(lsn.replace(":",""));
    }
    public static int compare(byte[] first,byte[] second) { return Arrays.compareUnsigned(first,second); }
    public static boolean covers(byte[] saved,byte[] minimum,byte[] end,byte[] maximum) {
        return minimum!=null&&maximum!=null&&compare(saved,minimum)>=0&&compare(saved,maximum)<=0&&(end==null||compare(saved,end)<=0);
    }
    @Override public synchronized void close() throws SQLException { connection.close(); }
    public record Diagnostics(int retentionMinutes,long headroomSeconds,String warning) {}
}
