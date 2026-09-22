package com.iqhr.cdc.store;

import com.iqhr.cdc.CdcProperties.Tenant;
import com.iqhr.cdc.model.*;
import com.iqhr.cdc.source.SqlServerControl;
import com.iqhr.cdc.source.SqlServerControl.TableObservation;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.*;

/** Admin owns desired/request fields; the leased reader owns observed and effective capture state. */
public final class TableRegistry {
    private final TenantStore store;
    private final Tenant configured;
    public TableRegistry(TenantStore store,Tenant configured) {this.store=store;this.configured=configured;}
    public static String id(Tenant tenant,String schema,String table) {
        return TenantStore.sha256(tenant.id()+"\n"+tenant.sourceId()+"\n"+schema+"\n"+table);
    }
    private List<CdcTable> rows(EntityManager em,boolean lock) {
        var query=em.createQuery("from CdcTable t where t.tenantId=:tenant and t.sourceId=:source order by t.tableId",CdcTable.class)
                .setParameter("tenant",configured.id()).setParameter("source",configured.sourceId());
        if(lock)query.setLockMode(LockModeType.PESSIMISTIC_WRITE);
        return query.getResultList();
    }
    public void initialize(List<TableObservation> observations) {
        store.transaction(em -> {
            SourceActivation activation=em.find(SourceActivation.class,configured.sourceId(),LockModeType.PESSIMISTIC_WRITE);
            if(!rows(em,true).isEmpty()) {if(activation!=null)activation.tableRegistryInitialized=true;return null;}
            if(activation!=null&&activation.tableRegistryInitialized)throw new ContinuityException("TABLE_REGISTRY_MISSING");
            if(activation!=null&&!TenantStore.configurationHash(configured).equals(activation.configurationHash))
                throw new ContinuityException("TABLE_REGISTRY_BOOTSTRAP_CONFIGURATION_MISMATCH");
            Map<String,TableObservation> found=new HashMap<>();observations.forEach(value -> found.put(value.qualifiedName(),value));
            for(String name:configured.tables()) {
                String[] parts=name.split("\\.",2);CdcTable row=newRow(parts[0],parts[1]);
                TableObservation observed=found.get(name);
                row.active=true;row.desiredEnabled=true;
                row.sqlCdcEnabled=observed!=null;row.captureFingerprint=observed==null?null:observed.fingerprint();
                row.state=observed==null?"MISSING":"ONBOARDING";
                row.diagnostic=observed==null?"SOURCE_TABLE_NOT_CDC_ENABLED":"AWAITING_CAPTURE_PROGRESS";
                em.persist(row);
            }
            if(activation!=null)activation.tableRegistryInitialized=true;
            return null;
        });
    }
    private CdcTable newRow(String schema,String table) {
        CdcTable row=new CdcTable();row.tableId=id(configured,schema,table);row.tenantId=configured.id();row.sourceId=configured.sourceId();
        row.schemaName=schema;row.tableName=table;row.state="DISCOVERED";row.observedAt=Instant.now();return row;
    }
    public Tenant effectiveTenant() {
        return store.transaction(em -> configured.withTables(rows(em,false).stream().filter(row -> row.active)
                .map(CdcTable::qualifiedName).sorted().toList()));
    }
    public Map<String,String> activationBoundaries() {
        return store.transaction(em -> {
            Map<String,String> result=new HashMap<>();
            rows(em,false).stream().filter(row -> row.active&&row.activationLsn!=null).forEach(row -> result.put(row.qualifiedName(),row.activationLsn));
            return Map.copyOf(result);
        });
    }
    /** Called only after a successful complete metadata read; failures retain the previous observation. */
    public void observe(List<TableObservation> observations,String savedLsn,long fence) {
        store.transaction(em -> {
            if(fence>0)store.assertFence(em,fence);
            List<CdcTable> current=rows(em,true);Map<String,CdcTable> byName=new HashMap<>();
            current.forEach(row -> byName.put(row.qualifiedName(),row));
            Set<String> seen=new HashSet<>();Instant now=Instant.now();
            for(TableObservation observed:observations) {
                CdcTable row=byName.get(observed.qualifiedName());
                if(row==null) {row=newRow(observed.schema(),observed.table());em.persist(row);}
                seen.add(observed.qualifiedName());row.sqlCdcEnabled=true;row.observedAt=now;
                if(!row.active&&!row.desiredEnabled) {
                    row.captureFingerprint=observed.fingerprint();row.state=observed.eligible()?"DISCOVERED":"BLOCKED";
                    row.diagnostic=observed.eligible()?"NOT_CONFIGURED":observed.diagnostic();continue;
                }
                if(row.captureFingerprint==null)row.captureFingerprint=observed.fingerprint();
                if(!row.captureFingerprint.equals(observed.fingerprint())) {
                    row.state="BLOCKED";row.diagnostic="CAPTURE_GENERATION_CHANGED";continue;
                }
                if("BLOCKED".equals(row.state))continue; // A failed continuity proof requires reviewed recovery.
                if(!observed.eligible()) {row.state="BLOCKED";row.diagnostic=observed.diagnostic();continue;}
                if(row.active&&savedLsn!=null&&!observed.coversStart(savedLsn)) {
                    row.state="BLOCKED";row.diagnostic="SOURCE_POSITION_UNAVAILABLE";continue;
                }
                if(row.active) {
                    if(!"ACTIVE".equals(row.state)) {row.state="ONBOARDING";row.diagnostic="AWAITING_CAPTURE_PROGRESS";}
                } else {row.state="PENDING";row.diagnostic="WAITING_SAFE_SOURCE_BOUNDARY";}
            }
            for(CdcTable row:current)if(!seen.contains(row.qualifiedName())) {
                row.sqlCdcEnabled=false;row.observedAt=now;
                // Keep the registered generation so re-enabling under the same name cannot erase the gap.
                row.state="MISSING";row.diagnostic="SOURCE_TABLE_NOT_CDC_ENABLED";
            }
            return null;
        });
    }
    public void assertActiveContinuity() {
        store.transaction(em -> {
            for(CdcTable row:rows(em,false))if(row.active&&(!row.sqlCdcEnabled||"BLOCKED".equals(row.state)||"MISSING".equals(row.state)))
                throw new ContinuityException(row.diagnostic==null?"ACTIVE_TABLE_UNAVAILABLE":row.diagnostic);
            return null;
        });
    }
    public List<String> readyRequests(List<TableObservation> observations,String savedLsn) {
        if(savedLsn==null)return List.of();
        Map<String,TableObservation> found=new HashMap<>();observations.forEach(value -> found.put(value.qualifiedName(),value));
        return store.transaction(em -> rows(em,false).stream().filter(row -> row.desiredEnabled&&!row.active&&"PENDING".equals(row.state))
                .filter(row -> found.containsKey(row.qualifiedName())&&found.get(row.qualifiedName()).coversStart(savedLsn))
                .map(CdcTable::qualifiedName).sorted().toList());
    }
    public boolean hasUnconfiguredTables() {
        return store.transaction(em -> rows(em,false).stream().anyMatch(row -> row.sqlCdcEnabled&&!row.desiredEnabled));
    }
    public void starting(long fence) {
        store.transaction(em -> {store.assertFence(em,fence);
            em.find(SourceActivation.class,configured.sourceId()).tableRegistryInitialized=true;
            for(CdcTable row:rows(em,true))if(row.active&&"ACTIVE".equals(row.state)) {row.state="ONBOARDING";row.diagnostic="AWAITING_CAPTURE_PROGRESS";}
            return null;
        });
    }
    public void confirmedProgress(long fence,String savedCommit) {
        store.transaction(em -> {store.assertFence(em,fence);
            for(CdcTable row:rows(em,true))if(row.active&&row.sqlCdcEnabled&&"ONBOARDING".equals(row.state)
                    &&(row.activationLsn==null||Arrays.compareUnsigned(SqlServerControl.parseLsn(savedCommit),SqlServerControl.parseLsn(row.activationLsn))>0)) {
                row.state="ACTIVE";row.diagnostic=null;if(row.enabledAt==null)row.enabledAt=Instant.now();
            }
            return null;
        });
    }
    /** Only invoked while the database lease is held and the preceding engine/consumer have fully stopped. */
    public boolean onboard(List<String> requested,Tenant previous,OffsetRow checkpoint,long expectedFence,String activationBoundary) {
        if(requested.isEmpty())return false;
        return store.transaction(em -> {
            SourceActivation activation=em.find(SourceActivation.class,configured.sourceId(),LockModeType.PESSIMISTIC_WRITE);
            if(activation==null||activation.fence!=expectedFence||!TenantStore.configurationHash(previous).equals(activation.configurationHash))
                throw new ContinuityException("SOURCE_FENCED");
            List<OffsetRow> offsets=em.createQuery("from OffsetRow",OffsetRow.class).setMaxResults(2).getResultList();
            if(offsets.size()!=1||!checkpoint.id.equals(offsets.getFirst().id)||!checkpoint.key.equals(offsets.getFirst().key)
                    ||!checkpoint.value.equals(offsets.getFirst().value))throw new ContinuityException("ONBOARDING_CHECKPOINT_CHANGED");
            List<CdcTable> current=rows(em,true);Set<String> matched=new HashSet<>();
            String savedCommit;
            try {savedCommit=new com.fasterxml.jackson.databind.ObjectMapper().readTree(checkpoint.value).path("commit_lsn").asText();}
            catch(java.io.IOException error) {throw new ContinuityException("SOURCE_OFFSETS_CORRUPT");}
            if(Arrays.compareUnsigned(SqlServerControl.parseLsn(activationBoundary),SqlServerControl.parseLsn(savedCommit))<0)
                throw new ContinuityException("ONBOARDING_BOUNDARY_BEFORE_CHECKPOINT");
            for(CdcTable row:current)if(requested.contains(row.qualifiedName())) {
                if(row.active||!row.desiredEnabled||!row.sqlCdcEnabled||!"PENDING".equals(row.state))
                    throw new ContinuityException("ONBOARDING_REQUEST_CHANGED");
                row.active=true;row.state="ONBOARDING";row.diagnostic="AWAITING_CAPTURE_PROGRESS";row.activationLsn=activationBoundary;
                matched.add(row.qualifiedName());
            }
            if(!matched.equals(new HashSet<>(requested)))throw new ContinuityException("ONBOARDING_REQUEST_CHANGED");
            Tenant next=configured.withTables(current.stream().filter(row -> row.active).map(CdcTable::qualifiedName).sorted().toList());
            activation.configurationHash=TenantStore.configurationHash(next);activation.fence=Math.addExact(activation.fence,1);
            return true;
        });
    }
}
