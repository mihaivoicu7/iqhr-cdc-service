package com.iqhr.cdc.maintenance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iqhr.cdc.CdcProperties.Tenant;
import com.iqhr.cdc.model.*;
import com.iqhr.cdc.source.RecoveryGuard;
import com.iqhr.cdc.source.SqlServerControl;
import com.iqhr.cdc.store.TenantStore;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import java.nio.file.*;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import org.hibernate.Session;

/** Explicit offline TECHNO incident recovery; never packaged or invoked by the live service. */
public final class OnboardTestTables {
    static final ObjectMapper MAPPER=new ObjectMapper().findAndRegisterModules();
    static final String SOURCE="employee-contract-info";
    static final List<String> OLD_TABLES=List.of("dbo.HR_Employee","dbo.HR_EmployeeContractInfo");
    static final List<String> NEXT_TABLES=List.of("dbo.HR_Employee","dbo.HR_EmployeeContractInfo",
            "dbo.HR_EmployeeAllocation","dbo.HR_EmployeeAddress");
    // Reviewed retained heartbeat UPDATE at 2026-09-22 11:24:48 UTC, before the missing address INSERT.
    // Debezium 3.2.7 counts the before/after CT pair as two operations at one commit/change position.
    static final String REPLAY_COMMIT="0006da1a:00000130:0003";
    static final String REPLAY_CHANGE="0006da1a:00000130:0002";
    static final long REPLAY_SERIAL=2;

    public static void main(String[] args) {
        try {
            require(args.length==1&&"--apply".equals(args[0]),"EXPLICIT_APPLY_REQUIRED");
            JsonNode config=MAPPER.readTree(Files.readString(path("CDC_TEST_CONFIG")));
            require("TECHNO".equals(config.path("tenantId").asText())
                    &&"192.168.99.133".equals(config.path("server").asText())
                    &&"tdev_technophar".equals(config.path("database").asText()),"WRONG_TEST_DATABASE");
            require(config.path("username").isTextual()&&config.path("password").isTextual(),"TEST_CREDENTIALS_REQUIRED");
            Tenant oldTenant=tenant(config,OLD_TABLES),nextTenant=tenant(config,NEXT_TABLES);
            Expected expected=Expected.read(MAPPER.readTree(Files.readString(path("CDC_CUTOVER_BACKUP"))));
            verifyReceipt(readReceipt(),Instant.now());
            try(SqlServerControl control=new SqlServerControl(nextTenant);TenantStore store=TenantStore.open(nextTenant)) {
                control.assertOwnership();
                RecoveryGuard.savedLsn(store.activation(),store.offsets(),oldTenant,MAPPER);
                control.inspect(REPLAY_COMMIT); // All five captures must still cover the selected replay boundary.
                verifyReceipt(readReceipt(),Instant.now());
                store.transaction(em -> {
                    control.assertOwnership();
                    applyEntityChanges(em,expected,oldTenant,nextTenant);
                    // CDC log metadata is SQL Server-specific; application state above uses mapped JPA entities.
                    em.unwrap(Session.class).doWork(OnboardTestTables::verifyRetainedBoundary);
                    em.flush();
                    verifyAfter(em,expected,nextTenant);
                    verifyReceipt(readReceipt(),Instant.now());
                    control.assertOwnership();
                    return null;
                });
                System.out.println(MAPPER.writeValueAsString(Map.of("committed",true,"sourceId",SOURCE,
                        "replayCommitLsn",REPLAY_COMMIT,"replayChangeLsn",REPLAY_CHANGE,"eventSerialNo",REPLAY_SERIAL,
                        "newHash",TenantStore.configurationHash(nextTenant),"newFence",expected.fence()+1,
                        "historySchemaAndIncarnationPreserved",true)));
            }
        } catch(Exception error) {
            System.err.println(safeCode(error));
            System.exit(1);
        }
    }

    static Tenant tenant(JsonNode config,List<String> tables) {
        return new Tenant("TECHNO",SOURCE,"192.168.99.133",1433,"tdev_technophar",config.path("username").asText(),
                config.path("password").asText(),false,true,tables,true,20160,1024,128);
    }
    static Path path(String name) {
        String value=System.getenv(name);require(value!=null&&!value.isBlank(),"MISSING_"+name);return Path.of(value);
    }
    static JsonNode readReceipt() {
        try {return MAPPER.readTree(Files.readString(path("CDC_DRAIN_RECEIPT")));}
        catch(java.io.IOException error) {throw new GuardFailure("UNREADABLE_DRAIN_RECEIPT");}
    }
    static void verifyReceipt(JsonNode receipt,Instant now) {
        require("iqhr.cdc.TECHNO.history".equals(receipt.path("queue").asText())
                &&"exited".equals(receipt.path("serviceState").asText()),"INVALID_STOPPED_SERVICE_RECEIPT");
        for(String field:List.of("MessageCount","DeliveringCount","ConsumerCount"))
            require(receipt.path(field).isIntegralNumber()&&receipt.path(field).longValue()==0,"BROKER_NOT_DRAINED");
        Instant checked;
        try {checked=Instant.parse(receipt.path("checkedAt").asText());}
        catch(RuntimeException error) {throw new GuardFailure("INVALID_RECEIPT_TIME");}
        long age=java.time.Duration.between(checked,now).toMillis();
        require(age>=0&&age<120000,"STALE_DRAIN_RECEIPT");
    }

    static void applyEntityChanges(EntityManager em,Expected expected,Tenant oldTenant,Tenant nextTenant) {
        var activations=em.createQuery("from SourceActivation",SourceActivation.class).setMaxResults(2).getResultList();
        require(activations.size()==1,"AMBIGUOUS_ACTIVATION");
        SourceActivation activation=em.find(SourceActivation.class,SOURCE,LockModeType.PESSIMISTIC_WRITE);
        require(activation!=null&&"TECHNO".equals(activation.tenantId)&&SOURCE.equals(activation.sourceId)
                &&activation.fence==expected.fence()&&expected.incarnation().equals(activation.incarnation)
                &&expected.oldHash().equals(activation.configurationHash)
                &&TenantStore.configurationHash(oldTenant).equals(activation.configurationHash),"ACTIVATION_CHANGED");
        OffsetRow offset=onlyOffset(em);
        require(expected.offsetId().equals(offset.id)&&expected.offsetKey().equals(offset.key)
                &&expected.offsetValue().equals(offset.value),"CHECKPOINT_CHANGED");
        RecoveryGuard.savedLsn(activation,List.of(offset),oldTenant,MAPPER);
        verifyHistoryAndSchema(em,expected);
        String replayValue=replayValue(offset.value);
        // A reviewed one-off exception to the mapping's normal library-only write ownership.
        // Updating the managed row preserves its key, ID and unmapped JDBC ordering columns.
        offset.value=replayValue;
        activation.configurationHash=TenantStore.configurationHash(nextTenant);
        activation.fence=Math.addExact(activation.fence,1);
    }

    static String replayValue(String value) {
        try {
            JsonNode parsed=MAPPER.readTree(value);
            require(parsed.isObject(),"INVALID_OFFSET_JSON");
            ObjectNode offset=(ObjectNode)parsed;
            require(!offset.has("snapshot")&&!offset.has("snapshot_completed"),"SNAPSHOT_OFFSET_NOT_SUPPORTED");
            byte[] commit=SqlServerControl.parseLsn(offset.path("commit_lsn").asText(null));
            byte[] change=SqlServerControl.parseLsn(offset.path("change_lsn").asText(null));
            require(offset.path("event_serial_no").isIntegralNumber()&&offset.path("event_serial_no").canConvertToLong()
                    &&offset.path("event_serial_no").longValue()>=0,"INVALID_OFFSET_SERIAL");
            int commitOrder=Arrays.compareUnsigned(commit,SqlServerControl.parseLsn(REPLAY_COMMIT));
            int changeOrder=Arrays.compareUnsigned(change,SqlServerControl.parseLsn(REPLAY_CHANGE));
            require(commitOrder>0||(commitOrder==0&&(changeOrder>0
                    ||(changeOrder==0&&offset.path("event_serial_no").longValue()>=REPLAY_SERIAL))),"CHECKPOINT_BEFORE_REPLAY_BOUNDARY");
            offset.put("commit_lsn",REPLAY_COMMIT);offset.put("change_lsn",REPLAY_CHANGE);offset.put("event_serial_no",REPLAY_SERIAL);
            return MAPPER.writeValueAsString(offset);
        } catch(java.io.IOException error) {throw new GuardFailure("INVALID_OFFSET_JSON");}
    }

    static OffsetRow onlyOffset(EntityManager em) {
        List<OffsetRow> offsets=em.createQuery("from OffsetRow",OffsetRow.class).setMaxResults(2).getResultList();
        require(offsets.size()==1,"CHECKPOINT_PARTITIONS_CHANGED");return offsets.getFirst();
    }
    static void verifyHistoryAndSchema(EntityManager em,Expected expected) {
        List<Long> sequences=em.createQuery("select e.sequence from SchemaHistoryRow e order by e.sequence",Long.class).getResultList();
        require(sequences.equals(expected.schemaSequences()),"SCHEMA_HISTORY_CHANGED");
        List<HistoryEvent> events=em.createQuery("from HistoryEvent e where e.tenantId=:tenant and e.sourceId=:source",HistoryEvent.class)
                .setParameter("tenant","TECHNO").setParameter("source",SOURCE).getResultList();
        require(new TreeSet<>(events.stream().map(event -> event.eventId).toList()).equals(expected.eventIds()),"HISTORY_CHANGED");
        for(HistoryEvent event:events) {
            require("dbo".equals(event.schemaName)&&OLD_TABLES.contains(event.schemaName+"."+event.tableName),"UNEXPECTED_EXISTING_HISTORY_TABLE");
            require(Arrays.compareUnsigned(SqlServerControl.parseLsn(event.commitLsn),SqlServerControl.parseLsn(REPLAY_COMMIT))<0,
                    "EXISTING_HISTORY_AT_OR_AFTER_REPLAY_BOUNDARY");
        }
    }
    static void verifyAfter(EntityManager em,Expected expected,Tenant nextTenant) {
        // Read back flushed state; do not let first-level cache conceal an unexpected storage trigger.
        em.clear();
        SourceActivation activation=em.find(SourceActivation.class,SOURCE);
        require(activation!=null&&expected.incarnation().equals(activation.incarnation)
                &&activation.fence==Math.addExact(expected.fence(),1)
                &&TenantStore.configurationHash(nextTenant).equals(activation.configurationHash),"ACTIVATION_WRITE_MISMATCH");
        OffsetRow offset=onlyOffset(em);
        require(expected.offsetId().equals(offset.id)&&expected.offsetKey().equals(offset.key)
                &&replayValue(expected.offsetValue()).equals(offset.value),"REPLAY_WRITE_MISMATCH");
        RecoveryGuard.savedLsn(activation,List.of(offset),nextTenant,MAPPER);
        verifyHistoryAndSchema(em,expected);
    }

    /** Native SQL reads only SQL Server-owned CDC replay metadata; no source/business writes or DDL. */
    static void verifyRetainedBoundary(Connection connection) throws SQLException {
        require(!connection.getAutoCommit(),"CDC_GUARDS_MUST_SHARE_JPA_TRANSACTION");
        List<HeartbeatRow> heartbeat=new ArrayList<>();
        try(PreparedStatement statement=connection.prepareStatement("SELECT [__$seqval],[__$operation],singleton_id FROM cdc.dbo_IQHR_CdcHeartbeat_CT WHERE [__$start_lsn]=? ORDER BY [__$seqval],[__$operation]")) {
            statement.setQueryTimeout(15);statement.setBytes(1,SqlServerControl.parseLsn(REPLAY_COMMIT));
            try(ResultSet rows=statement.executeQuery()) {
                while(rows.next()) {
                    require(heartbeat.size()<2,"AMBIGUOUS_HEARTBEAT_BOUNDARY");
                    heartbeat.add(new HeartbeatRow(rows.getBytes(1),rows.getInt(2),rows.getInt(3)));
                }
            }
        }
        verifyHeartbeat(heartbeat);
        // Fixed identifiers, deliberately no operator-supplied identifier interpolation.
        for(String table:List.of("cdc.dbo_HR_EmployeeAddress_CT","cdc.dbo_HR_EmployeeAllocation_CT"))
            requireNoRows(connection,table,"<=","NEW_TABLE_CHANGES_BEFORE_REPLAY_BOUNDARY");
        for(String table:List.of("cdc.dbo_HR_Employee_CT","cdc.dbo_HR_EmployeeContractInfo_CT"))
            requireNoRows(connection,table,"=","BUSINESS_CHANGE_AT_HEARTBEAT_BOUNDARY");
    }
    static void requireNoRows(Connection connection,String table,String comparison,String code) throws SQLException {
        Set<String> tables=Set.of("cdc.dbo_HR_EmployeeAddress_CT","cdc.dbo_HR_EmployeeAllocation_CT",
                "cdc.dbo_HR_Employee_CT","cdc.dbo_HR_EmployeeContractInfo_CT");
        require(tables.contains(table)&&Set.of("<=","=").contains(comparison),"INVALID_CDC_METADATA_IDENTIFIER");
        try(PreparedStatement statement=connection.prepareStatement("SELECT TOP(1) 1 FROM "+table+" WHERE [__$start_lsn]"+comparison+"?")) {
            statement.setQueryTimeout(15);statement.setBytes(1,SqlServerControl.parseLsn(REPLAY_COMMIT));
            try(ResultSet rows=statement.executeQuery()) {require(!rows.next(),code);}
        }
    }
    static void verifyHeartbeat(List<HeartbeatRow> rows) {
        require(rows.size()==2,"HEARTBEAT_BOUNDARY_MISSING");
        require(rows.get(0).operation()==3&&rows.get(1).operation()==4
                &&rows.stream().allMatch(row -> row.singletonId()==1
                &&Arrays.equals(row.changeLsn(),SqlServerControl.parseLsn(REPLAY_CHANGE))),"INVALID_HEARTBEAT_BOUNDARY");
    }
    record HeartbeatRow(byte[] changeLsn,int operation,int singletonId) {}

    static void require(boolean condition,String code) {if(!condition)throw new GuardFailure(code);}
    static String text(JsonNode object,String field) {
        JsonNode value=object.get(field);require(value!=null&&value.isTextual()&&!value.textValue().isBlank(),"INVALID_BACKUP_FIELD");return value.textValue();
    }
    static long positiveLong(JsonNode value,String code) {
        try {
            require(value!=null&&(value.isIntegralNumber()||value.isTextual())&&value.asText().matches("[0-9]+"),code);
            long parsed=Long.parseLong(value.asText());require(parsed>0,code);return parsed;
        } catch(NumberFormatException error) {throw new GuardFailure(code);}
    }
    static final class GuardFailure extends RuntimeException {GuardFailure(String code){super(code);}}
    static String safeCode(Throwable error) {
        StringBuilder result=new StringBuilder("ONBOARDING_FAILED");
        for(int depth=0;error!=null&&depth<6;error=error.getCause(),depth++) {
            if(error instanceof GuardFailure)return "ONBOARDING_REJECTED_"+error.getMessage();
            result.append('_').append(error.getClass().getSimpleName());
            if(error instanceof SQLException sql)result.append("_SQLCODE_").append(sql.getErrorCode());
        }
        return result.toString();
    }
    record Expected(String oldHash,long fence,String incarnation,String offsetId,String offsetKey,String offsetValue,
                    List<Long> schemaSequences,Set<String> eventIds) {
        static Expected read(JsonNode backup) {
            require(backup.isArray()&&backup.size()>=4&&backup.get(0).isArray()&&backup.get(0).size()==1
                    &&backup.get(1).isArray()&&backup.get(1).size()==1&&backup.get(2).isArray()&&backup.get(3).isArray(),"INVALID_BACKUP");
            JsonNode activation=backup.get(0).get(0),offset=backup.get(1).get(0);
            require("TECHNO".equals(activation.path("tenant_id").asText())&&SOURCE.equals(activation.path("source_id").asText()),"BACKUP_TENANT_MISMATCH");
            List<Long> sequences=new ArrayList<>();backup.get(2).forEach(row -> sequences.add(positiveLong(row.get("storage_sequence"),"INVALID_BACKUP_SCHEMA_SEQUENCE")));
            require(!sequences.isEmpty()&&new ArrayList<>(new TreeSet<>(sequences)).equals(sequences),"INVALID_BACKUP_SCHEMA_ORDER");
            Set<String> ids=new TreeSet<>();
            backup.get(3).forEach(row -> {
                require("TECHNO".equals(row.path("tenant_id").asText())&&SOURCE.equals(row.path("source_id").asText()),"BACKUP_HISTORY_SCOPE_MISMATCH");
                require(ids.add(text(row,"event_id")),"BACKUP_DUPLICATE_EVENT");
            });
            return new Expected(text(activation,"configuration_hash"),positiveLong(activation.get("fence"),"INVALID_BACKUP_FENCE"),
                    text(activation,"incarnation"),text(offset,"id"),text(offset,"offset_key"),text(offset,"offset_val"),List.copyOf(sequences),Set.copyOf(ids));
        }
    }
}
