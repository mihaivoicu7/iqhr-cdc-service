package com.iqhr.cdc.maintenance;

import com.iqhr.cdc.CdcProperties.Tenant;
import com.iqhr.cdc.model.*;
import com.iqhr.cdc.store.TenantStore;
import java.time.Instant;
import java.util.*;
import org.h2.jdbcx.JdbcDataSource;

/** Offline JPA transaction/scope regressions; never reads CDC_TEST_CONFIG or opens SQL Server. */
public final class CleanupTestProbesSelfTest {
    public static void main(String[] args) throws Exception {
        var stringBackup=CleanupTestProbes.MAPPER.readTree("""
            [[{"tenant_id":"TECHNO","source_id":"employee-contract-info","fence":"4","incarnation":"stable","configuration_hash":"hash"}],
             [{"id":"offset","offset_key":"key","offset_val":"value"}],
             [{"storage_sequence":"1"},{"storage_sequence":2}],[]]
            """);
        var parsedBackup=CleanupTestProbes.Expected.read(stringBackup);
        check(parsedBackup.fence()==4&&parsedBackup.schemaSequences().equals(List.of(1L,2L)),"mssql bigint string decoding");
        ((com.fasterxml.jackson.databind.node.ObjectNode)stringBackup.get(0).get(0)).put("fence","9223372036854775808");
        expect("INVALID_BACKUP_FENCE",() -> CleanupTestProbes.Expected.read(stringBackup));
        var config=CleanupTestProbes.MAPPER.readTree("{\"username\":\"unused\",\"password\":\"unused\"}");
        Tenant oldTenant=CleanupTestProbes.tenant(config,CleanupTestProbes.OLD_TABLES);
        Tenant nextTenant=CleanupTestProbes.tenant(config,CleanupTestProbes.NEXT_TABLES);
        JdbcDataSource ds=new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:"+UUID.randomUUID()+";MODE=MSSQLServer;DB_CLOSE_DELAY=-1;INIT=CREATE SCHEMA IF NOT EXISTS dbo");
        try(TenantStore store=new TenantStore(oldTenant,TenantStore.factory(ds,"org.hibernate.dialect.H2Dialect","create-only"))) {
            SourceActivation activation=store.claim();
            var expected=new CleanupTestProbes.Expected(activation.configurationHash,activation.fence,activation.incarnation,
                    "offset-id","key","value",List.of(1L),Set.of("probe-event"));
            store.transaction(em -> {
                OffsetRow offset=new OffsetRow();offset.id=expected.offsetId();offset.key=expected.offsetKey();offset.value=expected.offsetValue();em.persist(offset);
                SchemaHistoryRow schema=new SchemaHistoryRow();schema.sequence=1;em.persist(schema);
                em.persist(event("probe-event","TECHNO","CodexCdcProbe"));
                em.persist(event("real-info","TECHNO","HR_EmployeeContractInfo"));
                em.persist(event("foreign-probe","OTHER","CodexCdcProbe"));return null;
            });
            // A failed SQL Server DDL operation must roll back the already-flushed JPA deletes/hash/fence.
            expect("SIMULATED_DDL_FAILURE",() -> store.transaction(em -> {
                CleanupTestProbes.applyEntityChanges(em,expected,oldTenant,nextTenant);em.flush();
                throw new CleanupTestProbes.GuardFailure("SIMULATED_DDL_FAILURE");
            }));
            check(store.activation().configurationHash.equals(expected.oldHash()),"rollback hash");
            check(store.activation().fence==expected.fence(),"rollback fence");
            check(count(store)==3,"rollback fixture delete");
            var stale=new CleanupTestProbes.Expected(expected.oldHash(),expected.fence()+1,expected.incarnation(),expected.offsetId(),expected.offsetKey(),expected.offsetValue(),expected.schemaSequences(),expected.eventIds());
            expect("ACTIVATION_CHANGED",() -> store.transaction(em -> CleanupTestProbes.applyEntityChanges(em,stale,oldTenant,nextTenant)));
            var changedOffset=new CleanupTestProbes.Expected(expected.oldHash(),expected.fence(),expected.incarnation(),expected.offsetId(),expected.offsetKey(),"different",expected.schemaSequences(),expected.eventIds());
            expect("CHECKPOINT_CHANGED",() -> store.transaction(em -> CleanupTestProbes.applyEntityChanges(em,changedOffset,oldTenant,nextTenant)));
            int removed=store.transaction(em -> CleanupTestProbes.applyEntityChanges(em,expected,oldTenant,nextTenant));
            check(removed==1&&count(store)==2,"only matching fixture removed");
            check(store.activation().configurationHash.equals(TenantStore.configurationHash(nextTenant)),"next hash");
            check(store.activation().fence==expected.fence()+1,"fence advanced once");
            check(store.activation().incarnation.equals(expected.incarnation()),"incarnation preserved");
            store.transaction(em -> {CleanupTestProbes.verifyPreservedState(em,expected);return null;});
            expect("ACTIVATION_CHANGED",() -> store.transaction(em -> CleanupTestProbes.applyEntityChanges(em,expected,oldTenant,nextTenant)));
        }
        var receipt=CleanupTestProbes.MAPPER.readTree("{\"queue\":\"iqhr.cdc.TECHNO.history\",\"serviceState\":\"exited\",\"MessageCount\":0,\"DeliveringCount\":0,\"ConsumerCount\":0,\"checkedAt\":\"2026-09-22T10:00:00Z\"}");
        CleanupTestProbes.verifyReceipt(receipt,Instant.parse("2026-09-22T10:00:30Z"));
        expect("STALE_DRAIN_RECEIPT",() -> CleanupTestProbes.verifyReceipt(receipt,Instant.parse("2026-09-22T10:03:00Z")));
        ((com.fasterxml.jackson.databind.node.ObjectNode)receipt).put("MessageCount",1);
        expect("BROKER_NOT_DRAINED",() -> CleanupTestProbes.verifyReceipt(receipt,Instant.parse("2026-09-22T10:00:30Z")));
        System.out.println("PASS: JPA cleanup scope, rollback, hash/fence, checkpoint/schema/incarnation preservation, stale-state and drain receipt guards.");
    }
    static long count(TenantStore store) {return store.transaction(em -> em.createQuery("select count(e) from HistoryEvent e",Long.class).getSingleResult());}
    static HistoryEvent event(String id,String tenant,String table) {
        HistoryEvent event=new HistoryEvent();event.eventId=id;event.tenantId=tenant;event.sourceId=CleanupTestProbes.SOURCE;
        event.schemaName="dbo";event.tableName=table;event.operation="INSERT";event.occurredAt=Instant.now();event.capturedAt=Instant.now();
        event.recordKey="{\"id\":1}";event.afterJson="{}";event.changedProperties="[]";return event;
    }
    static void check(boolean condition,String description) {if(!condition)throw new AssertionError(description);}
    static void expect(String code,Runnable operation) {
        try {operation.run();throw new AssertionError("Expected "+code);}
        catch(CleanupTestProbes.GuardFailure error) {check(code.equals(error.getMessage()),"Unexpected guard code");}
    }
}
