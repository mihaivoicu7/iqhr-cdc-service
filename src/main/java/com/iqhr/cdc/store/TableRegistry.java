package com.iqhr.cdc.store;

import com.iqhr.cdc.CdcProperties.Tenant;
import com.iqhr.cdc.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iqhr.cdc.source.SchemaEvolution;
import com.iqhr.cdc.source.SqlServerControl;
import com.iqhr.cdc.source.SqlServerControl.TableObservation;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.*;

/** Admin owns desired/request fields; the leased reader owns observed and effective capture state. */
public final class TableRegistry {
    public static final String DISABLED_BY_ADMIN="DISABLED_BY_ADMIN";
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
    /**
     * Called only after a successful complete metadata read; failures retain the previous observation.
     * Continuity of an active table is proven by coverage: some capture instance still holds the saved position.
     * A changed capture shape or an added/replaced instance is therefore not a failure by itself (see
     * {@link SchemaEvolution}); a table whose history is gone is marked here and suspended alone afterwards.
     */
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
                row.captureFingerprint=observed.fingerprint();row.captureInstances=instances(observed);
                if(!row.active&&!row.desiredEnabled) {
                    row.state=observed.eligible()?"DISCOVERED":"BLOCKED";
                    row.diagnostic=observed.eligible()?"NOT_CONFIGURED":observed.diagnostic();continue;
                }
                if(!row.active) {
                    row.state=observed.eligible()?"PENDING":"BLOCKED";
                    row.diagnostic=observed.eligible()?"WAITING_SAFE_SOURCE_BOUNDARY":observed.diagnostic();continue;
                }
                if(!row.desiredEnabled) {row.state="STOPPING";row.diagnostic=DISABLED_BY_ADMIN;continue;}
                if(!observed.eligible()) {row.state="BLOCKED";row.diagnostic=observed.diagnostic();continue;}
                if(savedLsn!=null&&!observed.coversStart(savedLsn)) {row.state="BLOCKED";row.diagnostic="CAPTURE_HISTORY_GAP";continue;}
                if(!"ACTIVE".equals(row.state)) {row.state="ONBOARDING";row.diagnostic="AWAITING_CAPTURE_PROGRESS";}
            }
            for(CdcTable row:current)if(!seen.contains(row.qualifiedName())) {
                row.sqlCdcEnabled=false;row.observedAt=now;row.captureInstances=null;
                row.state="MISSING";row.diagnostic="SOURCE_TABLE_NOT_CDC_ENABLED";
            }
            return null;
        });
    }
    private static String instances(TableObservation observed) {
        String value=String.join(",",observed.captures().stream().map(SqlServerControl.Capture::name).toList());
        return value.isEmpty()?null:value.length()>1000?value.substring(0,1000):value;
    }
    /** First activation has no checkpoint to keep other tables running from; it stays fail-closed. */
    public void assertActiveContinuity() {
        store.transaction(em -> {
            for(CdcTable row:rows(em,false))if(row.active&&unavailable(row))
                throw new ContinuityException(row.diagnostic==null?"ACTIVE_TABLE_UNAVAILABLE":row.diagnostic);
            return null;
        });
    }
    private static boolean unavailable(CdcTable row) {
        return !row.sqlCdcEnabled||"BLOCKED".equals(row.state)||"MISSING".equals(row.state)||"STOPPING".equals(row.state);
    }
    /** An active table must leave the reader configuration: lost history, CDC disabled, or stopped by Admin. */
    public boolean hasUnavailableActive() {
        return store.transaction(em -> rows(em,false).stream().anyMatch(row -> row.active&&unavailable(row)));
    }
    /**
     * Removes unavailable tables from the stopped reader's configuration so every other table keeps capturing.
     * The table keeps its history; the gap is recorded (reason and saved position) until Admin re-enables or
     * acknowledges it. Only invoked while no engine runs, under the lease and the expected fence.
     */
    public boolean suspendUnavailable(Tenant current,long expectedFence,String savedLsn) {
        return store.transaction(em -> {
            SourceActivation activation=em.find(SourceActivation.class,configured.sourceId(),LockModeType.PESSIMISTIC_WRITE);
            if(activation==null||activation.fence!=expectedFence||!TenantStore.configurationHash(current).equals(activation.configurationHash))
                throw new ContinuityException("SOURCE_FENCED");
            List<CdcTable> rows=rows(em,true);Instant now=Instant.now();boolean changed=false;
            for(CdcTable row:rows)if(row.active&&unavailable(row)) {
                row.suspensionReason=!row.desiredEnabled?DISABLED_BY_ADMIN:row.diagnostic==null?"ACTIVE_TABLE_UNAVAILABLE":row.diagnostic;
                row.active=false;row.desiredEnabled=false;row.suspendedAt=now;row.suspendedLsn=savedLsn;row.activationLsn=null;
                row.state=row.sqlCdcEnabled?"DISCOVERED":"MISSING";row.diagnostic=row.sqlCdcEnabled?"NOT_CONFIGURED":"SOURCE_TABLE_NOT_CDC_ENABLED";
                changed=true;
            }
            if(!changed)return false;
            List<String> remaining=rows.stream().filter(row -> row.active).map(CdcTable::qualifiedName).sorted().toList();
            if(remaining.isEmpty())throw new ContinuityException("ALL_CAPTURED_TABLES_UNAVAILABLE");
            activation.configurationHash=TenantStore.configurationHash(configured.withTables(remaining));
            activation.fence=Math.addExact(activation.fence,1);
            return true;
        });
    }
    /** Suspensions other than an Admin stop that nobody acknowledged yet; reported as a degraded source. */
    public boolean hasUnacknowledgedSuspension() {
        return store.transaction(em -> rows(em,false).stream().anyMatch(row -> !row.active&&row.suspendedAt!=null
                &&!DISABLED_BY_ADMIN.equals(row.suspensionReason)&&(row.requestedAt==null||row.requestedAt.isBefore(row.suspendedAt))));
    }
    /** An Admin request recorded after the suspension (re-enable or stop) acknowledges it. */
    private static void acknowledge(CdcTable row) {
        if(row.suspendedAt!=null&&row.requestedAt!=null&&row.requestedAt.isAfter(row.suspendedAt)&&!row.active&&!row.desiredEnabled)
            row.suspensionReason=DISABLED_BY_ADMIN;
    }

    /**
     * Records each table's schema assessment and returns the capture-instance actions to run now. Actions are
     * marked as started before they run, so a crash cannot repeat an automatic refresh in a loop.
     */
    public List<SchemaEvolution.Action> planSchemaEvolution(List<TableObservation> observations,Map<String,List<SqlServerControl.Column>> reader,
                                                           String savedLsn,long fence,boolean automatic,ObjectMapper mapper) {
        return store.transaction(em -> {
            if(fence>0)store.assertFence(em,fence);
            Map<String,CdcTable> byName=new HashMap<>();rows(em,true).forEach(row -> {acknowledge(row);byName.put(row.qualifiedName(),row);});
            List<SchemaEvolution.Action> actions=new ArrayList<>();Instant now=Instant.now();
            for(TableObservation observed:observations) {
                CdcTable row=byName.get(observed.qualifiedName());
                if(row==null)continue;
                boolean capturing=row.active&&("ACTIVE".equals(row.state)||"ONBOARDING".equals(row.state));
                var assessment=SchemaEvolution.assess(observed,capturing?reader.get(observed.qualifiedName()):null);
                String detail=SchemaEvolution.detail(assessment,mapper);
                row.schemaDetail=detail;
                if(!capturing) {row.schemaState=assessment.state();row.refreshRequestedAt=null;continue;}
                var action=SchemaEvolution.plan(observed,assessment,savedLsn,row.refreshRequestedAt!=null,automatic,
                        detail,row.refreshDetail,row.refreshStartedAt,now);
                if(action.orElse(null) instanceof SchemaEvolution.Create) {
                    row.refreshStartedAt=now;row.refreshDetail=detail;row.refreshRequestedAt=null;row.refreshError=null;
                    row.schemaState=SchemaEvolution.REFRESHING;
                } else {
                    // A request during an ongoing replacement is satisfied by that replacement.
                    if(SchemaEvolution.REFRESHING.equals(assessment.state()))row.refreshRequestedAt=null;
                    row.schemaState=row.refreshError!=null&&!SchemaEvolution.CURRENT.equals(assessment.state())?SchemaEvolution.REFRESH_FAILED:assessment.state();
                }
                action.ifPresent(actions::add);
            }
            return actions;
        });
    }
    public void schemaActionCompleted(SchemaEvolution.Action action,long fence) {
        update(action,fence,row -> {if(action instanceof SchemaEvolution.Retire){row.refreshedAt=Instant.now();}row.refreshError=null;});
    }
    public void schemaActionFailed(SchemaEvolution.Action action,long fence,String code) {
        update(action,fence,row -> {row.refreshError=code;row.schemaState=SchemaEvolution.REFRESH_FAILED;});
    }
    /** Active tables whose capture refresh needs an operator (for example missing db_owner permission). */
    public boolean hasFailedRefresh() {
        return store.transaction(em -> rows(em,false).stream().anyMatch(row -> row.active&&SchemaEvolution.REFRESH_FAILED.equals(row.schemaState)));
    }
    private void update(SchemaEvolution.Action action,long fence,java.util.function.Consumer<CdcTable> change) {
        store.transaction(em -> {
            if(fence>0)store.assertFence(em,fence);
            CdcTable row=em.find(CdcTable.class,id(configured,action.schema(),action.table()),LockModeType.PESSIMISTIC_WRITE);
            if(row!=null)change.accept(row);
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
                row.suspendedAt=null;row.suspensionReason=null;row.suspendedLsn=null;
                matched.add(row.qualifiedName());
            }
            if(!matched.equals(new HashSet<>(requested)))throw new ContinuityException("ONBOARDING_REQUEST_CHANGED");
            Tenant next=configured.withTables(current.stream().filter(row -> row.active).map(CdcTable::qualifiedName).sorted().toList());
            activation.configurationHash=TenantStore.configurationHash(next);activation.fence=Math.addExact(activation.fence,1);
            return true;
        });
    }
}
