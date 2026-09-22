package com.iqhr.cdc.maintenance;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iqhr.cdc.CdcProperties.Tenant;
import com.iqhr.cdc.model.*;
import com.iqhr.cdc.source.SqlServerControl;
import com.iqhr.cdc.store.TenantStore;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import org.h2.jdbcx.JdbcDataSource;
import org.hibernate.Session;

/** Offline H2/JPA and CDC-boundary regressions. Never reads private config or connects to SQL Server. */
public final class OnboardTestTablesSelfTest {
    static final String OLD_COMMIT="0006da1b:000000a8:0003";
    static final String OLD_CHANGE="0006da1b:000000a8:0002";
    static final List<String> CAPTURES=List.of("IQHR_CdcHeartbeat","HR_Employee","HR_EmployeeContractInfo","HR_EmployeeAddress","HR_EmployeeAllocation");

    public static void main(String[] args) throws Exception {
        checkBackupAndReceipt();
        var config=OnboardTestTables.MAPPER.readTree("{\"username\":\"unused\",\"password\":\"unused\"}");
        Tenant oldTenant=OnboardTestTables.tenant(config,OnboardTestTables.OLD_TABLES);
        Tenant nextTenant=OnboardTestTables.tenant(config,OnboardTestTables.NEXT_TABLES);
        String key=OnboardTestTables.MAPPER.writeValueAsString(List.of(oldTenant.connectorName(),
                Map.of("server",oldTenant.connectorName(),"database",oldTenant.database())));
        String value=OnboardTestTables.MAPPER.writeValueAsString(Map.of("commit_lsn",OLD_COMMIT,"change_lsn",OLD_CHANGE,
                "event_serial_no",2,"additional_state",Map.of("retained",List.of(1,2,3))));
        checkOffset(value);
        JdbcDataSource ds=new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:"+UUID.randomUUID()+";MODE=MSSQLServer;DB_CLOSE_DELAY=-1;INIT=CREATE SCHEMA IF NOT EXISTS dbo");
        try(TenantStore store=new TenantStore(oldTenant,TenantStore.factory(ds,"org.hibernate.dialect.H2Dialect","create-only"))) {
            createMetadataFixtures(ds);
            SourceActivation activation=store.claim();Instant activationTime=activation.createdAt;
            var expected=new OnboardTestTables.Expected(activation.configurationHash,activation.fence,activation.incarnation,
                    "offset-id",key,value,List.of(1L,2L),Set.of("existing-info"));
            store.transaction(em -> {
                OffsetRow offset=new OffsetRow();offset.id=expected.offsetId();offset.key=key;offset.value=value;em.persist(offset);
                for(long sequence:expected.schemaSequences()) {SchemaHistoryRow row=new SchemaHistoryRow();row.sequence=sequence;em.persist(row);}
                em.persist(event("existing-info","TECHNO","0006da0d:000002b8:001a"));
                em.persist(event("foreign-event","OTHER",OLD_COMMIT));return null;
            });
            // Native metadata guard failure after a flush must roll back BOTH checkpoint and activation.
            expect("HEARTBEAT_BOUNDARY_MISSING",() -> store.transaction(em -> {
                OnboardTestTables.applyEntityChanges(em,expected,oldTenant,nextTenant);em.flush();
                em.unwrap(Session.class).doWork(OnboardTestTables::verifyRetainedBoundary);return null;
            }));
            checkOriginal(store,expected);
            insert(ds,"IQHR_CdcHeartbeat",OnboardTestTables.REPLAY_COMMIT,OnboardTestTables.REPLAY_CHANGE,3,1);
            insert(ds,"IQHR_CdcHeartbeat",OnboardTestTables.REPLAY_COMMIT,OnboardTestTables.REPLAY_CHANGE,4,1);
            store.transaction(em -> {em.unwrap(Session.class).doWork(OnboardTestTables::verifyRetainedBoundary);return null;});
            for(String table:List.of("HR_EmployeeAddress","HR_EmployeeAllocation")) {
                insert(ds,table,OnboardTestTables.REPLAY_COMMIT,OnboardTestTables.REPLAY_CHANGE,2,1);
                expect("NEW_TABLE_CHANGES_BEFORE_REPLAY_BOUNDARY",() -> store.transaction(em -> {
                    em.unwrap(Session.class).doWork(OnboardTestTables::verifyRetainedBoundary);return null;
                }));
                clear(ds,table);
            }
            insert(ds,"HR_EmployeeAddress","0006da12:00000128:00a5",OnboardTestTables.REPLAY_CHANGE,2,1);
            expect("NEW_TABLE_CHANGES_BEFORE_REPLAY_BOUNDARY",() -> store.transaction(em -> {
                em.unwrap(Session.class).doWork(OnboardTestTables::verifyRetainedBoundary);return null;
            }));
            clear(ds,"HR_EmployeeAddress");
            insert(ds,"HR_EmployeeAddress","0006da1a:00000188:0008","0006da1a:00000188:0007",2,1);
            insert(ds,"HR_Employee",OnboardTestTables.REPLAY_COMMIT,OnboardTestTables.REPLAY_CHANGE,2,1);
            expect("BUSINESS_CHANGE_AT_HEARTBEAT_BOUNDARY",() -> store.transaction(em -> {
                em.unwrap(Session.class).doWork(OnboardTestTables::verifyRetainedBoundary);return null;
            }));
            clear(ds,"HR_Employee");
            var stale=new OnboardTestTables.Expected(expected.oldHash(),expected.fence()+1,expected.incarnation(),expected.offsetId(),key,value,expected.schemaSequences(),expected.eventIds());
            expect("ACTIVATION_CHANGED",() -> store.transaction(em -> {OnboardTestTables.applyEntityChanges(em,stale,oldTenant,nextTenant);return null;}));
            var changedOffset=new OnboardTestTables.Expected(expected.oldHash(),expected.fence(),expected.incarnation(),expected.offsetId(),key,"different",expected.schemaSequences(),expected.eventIds());
            expect("CHECKPOINT_CHANGED",() -> store.transaction(em -> {OnboardTestTables.applyEntityChanges(em,changedOffset,oldTenant,nextTenant);return null;}));
            var changedSchema=new OnboardTestTables.Expected(expected.oldHash(),expected.fence(),expected.incarnation(),expected.offsetId(),key,value,List.of(1L),expected.eventIds());
            expect("SCHEMA_HISTORY_CHANGED",() -> store.transaction(em -> {OnboardTestTables.applyEntityChanges(em,changedSchema,oldTenant,nextTenant);return null;}));
            var changedHistory=new OnboardTestTables.Expected(expected.oldHash(),expected.fence(),expected.incarnation(),expected.offsetId(),key,value,expected.schemaSequences(),Set.of());
            expect("HISTORY_CHANGED",() -> store.transaction(em -> {OnboardTestTables.applyEntityChanges(em,changedHistory,oldTenant,nextTenant);return null;}));
            expect("EXISTING_HISTORY_AT_OR_AFTER_REPLAY_BOUNDARY",() -> store.transaction(em -> {
                em.find(HistoryEvent.class,"existing-info").commitLsn=OnboardTestTables.REPLAY_COMMIT;em.flush();
                OnboardTestTables.applyEntityChanges(em,expected,oldTenant,nextTenant);return null;
            }));
            checkOriginal(store,expected);
            store.transaction(em -> {
                OnboardTestTables.applyEntityChanges(em,expected,oldTenant,nextTenant);
                em.unwrap(Session.class).doWork(OnboardTestTables::verifyRetainedBoundary);
                em.flush();OnboardTestTables.verifyAfter(em,expected,nextTenant);return null;
            });
            check(store.activation().fence==expected.fence()+1,"fence increments once");
            check(store.activation().incarnation.equals(expected.incarnation()),"incarnation preserved");
            check(store.activation().createdAt.equals(activationTime),"activation time preserved");
            check(store.activation().configurationHash.equals(TenantStore.configurationHash(nextTenant)),"new allowlist hash");
            check(store.offsets().getFirst().value.equals(OnboardTestTables.replayValue(value)),"selected exact checkpoint");
            check(store.transaction(em -> em.createQuery("select count(e) from HistoryEvent e",Long.class).getSingleResult())==2,"all history preserved");
            check(store.transaction(em -> em.find(HistoryEvent.class,"existing-info").afterJson).equals("{\"stable\":true}"),"history payload preserved");
            try(Connection connection=ds.getConnection();Statement statement=connection.createStatement();ResultSet rows=statement.executeQuery("SELECT record_insert_seq FROM dbo.IQHR_CdcOffset")) {
                check(rows.next()&&rows.getLong(1)==77,"unmapped offset ordering column preserved");
            }
            expect("ACTIVATION_CHANGED",() -> store.transaction(em -> {OnboardTestTables.applyEntityChanges(em,expected,oldTenant,nextTenant);return null;}));
        }
        byte[] change=SqlServerControl.parseLsn(OnboardTestTables.REPLAY_CHANGE);
        OnboardTestTables.verifyHeartbeat(List.of(new OnboardTestTables.HeartbeatRow(change,3,1),new OnboardTestTables.HeartbeatRow(change,4,1)));
        expect("INVALID_HEARTBEAT_BOUNDARY",() -> OnboardTestTables.verifyHeartbeat(List.of(new OnboardTestTables.HeartbeatRow(change,3,2),new OnboardTestTables.HeartbeatRow(change,4,1))));
        expect("INVALID_HEARTBEAT_BOUNDARY",() -> OnboardTestTables.verifyHeartbeat(List.of(new OnboardTestTables.HeartbeatRow(change,2,1),new OnboardTestTables.HeartbeatRow(change,4,1))));
        expect("INVALID_HEARTBEAT_BOUNDARY",() -> OnboardTestTables.verifyHeartbeat(List.of(new OnboardTestTables.HeartbeatRow(SqlServerControl.parseLsn(OLD_CHANGE),3,1),new OnboardTestTables.HeartbeatRow(change,4,1))));
        String diagnostic=OnboardTestTables.safeCode(new SQLException("secret row contents/password","S1000",51000));
        check(!diagnostic.contains("secret")&&diagnostic.contains("51000"),"sanitized errors");
        System.out.println("PASS: offline onboarding JPA atomicity, exact state guards, retained CDC boundary/serial, history/schema/incarnation preservation, bigint backup parsing and fresh empty-queue receipt.");
    }

    static void checkBackupAndReceipt() throws Exception {
        var backup=OnboardTestTables.MAPPER.readTree("""
            [[{"tenant_id":"TECHNO","source_id":"employee-contract-info","fence":"6","incarnation":"stable","configuration_hash":"hash"}],
             [{"id":"offset","offset_key":"key","offset_val":"value"}],
             [{"storage_sequence":"1"},{"storage_sequence":2}],[]]
            """);
        var expected=OnboardTestTables.Expected.read(backup);
        check(expected.fence()==6&&expected.schemaSequences().equals(List.of(1L,2L)),"SQL bigint strings");
        ((ObjectNode)backup.get(0).get(0)).put("fence","9223372036854775808");
        expect("INVALID_BACKUP_FENCE",() -> OnboardTestTables.Expected.read(backup));
        var receipt=(ObjectNode)OnboardTestTables.MAPPER.readTree("{\"queue\":\"iqhr.cdc.TECHNO.history\",\"serviceState\":\"exited\",\"MessageCount\":0,\"DeliveringCount\":0,\"ConsumerCount\":0,\"checkedAt\":\"2026-09-22T11:30:00Z\"}");
        Instant now=Instant.parse("2026-09-22T11:30:30Z");
        OnboardTestTables.verifyReceipt(receipt,now);
        expect("STALE_DRAIN_RECEIPT",() -> OnboardTestTables.verifyReceipt(receipt,now.plusSeconds(121)));
        expect("STALE_DRAIN_RECEIPT",() -> OnboardTestTables.verifyReceipt(receipt,now.minusSeconds(60)));
        receipt.put("DeliveringCount",1);expect("BROKER_NOT_DRAINED",() -> OnboardTestTables.verifyReceipt(receipt,now));
        receipt.put("DeliveringCount",0);receipt.put("ConsumerCount",1);expect("BROKER_NOT_DRAINED",() -> OnboardTestTables.verifyReceipt(receipt,now));
        receipt.put("ConsumerCount",0);receipt.put("serviceState","running");expect("INVALID_STOPPED_SERVICE_RECEIPT",() -> OnboardTestTables.verifyReceipt(receipt,now));
    }
    static void checkOffset(String value) throws Exception {
        var replay=OnboardTestTables.MAPPER.readTree(OnboardTestTables.replayValue(value));
        check(replay.path("commit_lsn").asText().equals(OnboardTestTables.REPLAY_COMMIT)
                &&replay.path("change_lsn").asText().equals(OnboardTestTables.REPLAY_CHANGE)
                &&replay.path("event_serial_no").longValue()==2,"exact full replay tuple");
        check(replay.path("additional_state").equals(OnboardTestTables.MAPPER.readTree(value).path("additional_state")),"unknown offset fields preserved");
        ObjectNode snapshot=(ObjectNode)OnboardTestTables.MAPPER.readTree(value);snapshot.put("snapshot",false);
        expect("SNAPSHOT_OFFSET_NOT_SUPPORTED",() -> OnboardTestTables.replayValue(snapshot.toString()));
        snapshot.remove("snapshot");snapshot.putNull("snapshot_completed");
        expect("SNAPSHOT_OFFSET_NOT_SUPPORTED",() -> OnboardTestTables.replayValue(snapshot.toString()));
        ObjectNode earlier=(ObjectNode)replay.deepCopy();earlier.put("event_serial_no",1);
        expect("CHECKPOINT_BEFORE_REPLAY_BOUNDARY",() -> OnboardTestTables.replayValue(earlier.toString()));
        earlier.put("commit_lsn","0006da19:00000000:0001");earlier.put("event_serial_no",2);
        expect("CHECKPOINT_BEFORE_REPLAY_BOUNDARY",() -> OnboardTestTables.replayValue(earlier.toString()));
        earlier.put("event_serial_no","2");expect("INVALID_OFFSET_SERIAL",() -> OnboardTestTables.replayValue(earlier.toString()));
    }
    static void createMetadataFixtures(JdbcDataSource ds) throws SQLException {
        try(Connection connection=ds.getConnection();Statement statement=connection.createStatement()) {
            statement.execute("ALTER TABLE dbo.IQHR_CdcOffset ADD record_insert_seq BIGINT DEFAULT 77");
            statement.execute("CREATE SCHEMA cdc");
            for(String table:CAPTURES)statement.execute("CREATE TABLE cdc.dbo_"+table+"_CT ([__$start_lsn] VARBINARY(10),[__$seqval] VARBINARY(10),[__$operation] INT,singleton_id INT)");
        }
    }
    static void insert(JdbcDataSource ds,String table,String commit,String change,int operation,int singleton) throws SQLException {
        check(CAPTURES.contains(table),"fixed fixture identifier");
        try(Connection connection=ds.getConnection();PreparedStatement statement=connection.prepareStatement("INSERT INTO cdc.dbo_"+table+"_CT VALUES(?,?,?,?)")) {
            statement.setBytes(1,SqlServerControl.parseLsn(commit));statement.setBytes(2,SqlServerControl.parseLsn(change));
            statement.setInt(3,operation);statement.setInt(4,singleton);statement.executeUpdate();
        }
    }
    static void clear(JdbcDataSource ds,String table) throws SQLException {
        check(CAPTURES.contains(table),"fixed fixture identifier");
        try(Connection connection=ds.getConnection();Statement statement=connection.createStatement()) {statement.executeUpdate("DELETE FROM cdc.dbo_"+table+"_CT");}
    }
    static void checkOriginal(TenantStore store,OnboardTestTables.Expected expected) {
        check(store.activation().configurationHash.equals(expected.oldHash())&&store.activation().fence==expected.fence(),"activation rollback");
        check(store.offsets().getFirst().value.equals(expected.offsetValue()),"checkpoint rollback");
    }
    static HistoryEvent event(String id,String tenant,String commit) {
        HistoryEvent event=new HistoryEvent();event.eventId=id;event.tenantId=tenant;event.sourceId=OnboardTestTables.SOURCE;
        event.schemaName="dbo";event.tableName="HR_EmployeeContractInfo";event.operation="UPDATE";
        event.occurredAt=Instant.now();event.capturedAt=Instant.now();event.recordKey="{\"EmployeeID\":1,\"StartDate\":\"2026-01-01\"}";
        event.beforeJson="{}";event.afterJson="{\"stable\":true}";event.changedProperties="[\"stable\"]";
        event.commitLsn=commit;event.changeLsn=OLD_CHANGE;event.eventSerialNo=2L;return event;
    }
    static void check(boolean condition,String message) {if(!condition)throw new AssertionError(message);}
    static void expect(String code,Runnable operation) {
        try {operation.run();throw new AssertionError("Expected "+code);}
        catch(OnboardTestTables.GuardFailure error) {check(code.equals(error.getMessage()),"Unexpected guard: "+error.getMessage());}
    }
}
