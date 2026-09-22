package com.iqhr.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iqhr.cdc.broker.*;
import com.iqhr.cdc.model.*;
import com.iqhr.cdc.source.*;
import com.iqhr.cdc.store.*;
import io.debezium.engine.*;
import io.debezium.engine.format.Json;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A failed tenant never stops another tenant. All durable progress survives process restarts. */
public final class TenantPipeline implements AutoCloseable {
    private static final Logger LOG=LoggerFactory.getLogger(TenantPipeline.class);
    private final CdcProperties settings;
    private final CdcProperties.Tenant configuredTenant;
    private volatile CdcProperties.Tenant tenant;
    private final ObjectMapper mapper;
    private final AtomicBoolean running=new AtomicBoolean(true);
    private volatile String state="STARTING", diagnostic="INITIALIZING";
    private volatile Instant updatedAt=Instant.now();
    private volatile Thread worker;
    private volatile DebeziumEngine<ChangeEvent<String,String>> engine;
    public TenantPipeline(CdcProperties settings,CdcProperties.Tenant tenant,ObjectMapper mapper) {
        this.settings=settings; this.configuredTenant=tenant; this.tenant=tenant; this.mapper=mapper;
    }
    public void start() { worker=Thread.ofVirtual().name("cdc-tenant-"+tenant.id()).start(this::supervise); }
    private void supervise() {
        while(running.get()) {
            try { runOnce(); }
            catch(Exception e) {
                state="ERROR"; diagnostic=code(e); updatedAt=Instant.now();
                LOG.warn("CDC tenant={} state=ERROR code={} exception={}",tenant.id(),diagnostic,e.getClass().getSimpleName());
            }
            if(running.get())pause(settings.retryDelay());
        }
        state="STOPPED"; updatedAt=Instant.now();
    }
    private void runOnce() throws Exception {
        // Acquire the SQL Server session lock before migrations or activation writes.
        try(SqlServerControl control=new SqlServerControl(configuredTenant); TenantStore store=TenantStore.open(configuredTenant);
            DurableBroker broker=new DurableBroker(settings.broker(),configuredTenant)) {
            long fence=0;
            try {
                TableRegistry registry=new TableRegistry(store,configuredTenant);
                List<SqlServerControl.TableObservation> observed=control.observeTables();
                registry.initialize(observed);
                tenant=registry.effectiveTenant();
                SourceActivation previous=store.activation();
                String savedLsn=RecoveryGuard.savedLsn(previous,store.offsets(),tenant,mapper);
                if(previous!=null&&!store.hasSchemaHistory())throw new ContinuityException("SCHEMA_HISTORY_MISSING");
                if(previous==null&&(store.hasHistory()||store.hasSchemaHistory()))throw new ContinuityException("ACTIVATION_STATE_INCONSISTENT");
                registry.observe(observed,savedLsn,0);registry.assertActiveContinuity();
                // A stopped-reader request is enrolled at its existing checkpoint before any engine can advance it.
                if(previous!=null&&tryOnboard(store,control,registry,observed,previous.fence)) {
                    tenant=registry.effectiveTenant();previous=store.activation();
                    savedLsn=RecoveryGuard.savedLsn(previous,store.offsets(),tenant,mapper);
                }
                SqlServerControl.Diagnostics initial=control.inspect(savedLsn,tenant.capturedTables());
                if(initial.retentionMinutes()<tenant.minimumRetentionMinutes())throw new ContinuityException("CDC_RETENTION_BELOW_14_DAYS");
                broker.verifyQueue(); // MUST precede the first activation and engine start.
                SourceActivation activation=store.claim(tenant); fence=activation.fence;
                registry.starting(fence);
                boolean requested=captureAndConsume(store,control,broker,activation,previous==null,registry);
                if(requested) {
                    // captureAndConsume has joined both workers and flushed confirmed offsets before returning.
                    observed=control.observeTables();
                    savedLsn=RecoveryGuard.savedLsn(activation,store.offsets(),tenant,mapper);
                    registry.observe(observed,savedLsn,fence);registry.assertActiveContinuity();
                    tryOnboard(store,control,registry,observed,fence);
                }
            } catch(Exception e) {
                state="ERROR"; diagnostic=code(e); updatedAt=Instant.now();
                // Never overwrite another live owner's status before having acquired the session lock.
                writeStatus(store,fence,"ERROR","ERROR",null,null,null);
                throw e;
            }
        }
    }
    boolean tryOnboard(TenantStore store,SqlServerControl control,TableRegistry registry,
                               List<SqlServerControl.TableObservation> observed,long fence) throws Exception {
        SourceActivation activation=store.activation();
        List<OffsetRow> offsets=store.offsets();
        String saved=RecoveryGuard.savedLsn(activation,offsets,tenant,mapper);
        List<String> requested=registry.readyRequests(observed,saved);
        if(requested.isEmpty())return false;
        var offset=mapper.readTree(offsets.getFirst().value);
        if(offset.has("snapshot")||offset.has("snapshot_completed"))return false;
        List<String> expanded=new ArrayList<>(tenant.tables());expanded.addAll(requested);
        control.inspect(saved,tenant.withTables(expanded).capturedTables());
        if(!control.boundaryHasNoNewRows(requested,saved,offset.path("change_lsn").asText()))return false;
        // fn_cdc_get_max_lsn alone may lag pre-request commits. Wait for our post-request internal tick.
        long tick=store.tickHeartbeat(fence);
        long deadline=System.nanoTime()+Duration.ofSeconds(30).toNanos();String boundary=null;
        while(running.get()&&System.nanoTime()<deadline&&(boundary=control.onboardingBoundary(tick))==null)Thread.sleep(500);
        if(boundary==null)return false;
        control.inspect(boundary,tenant.withTables(expanded).capturedTables());
        // Recheck generations after the barrier wait; a concurrent DDL/capture replacement must block enrollment.
        List<SqlServerControl.TableObservation> latest=control.observeTables();
        registry.observe(latest,saved,fence);registry.assertActiveContinuity();
        if(!registry.readyRequests(latest,saved).containsAll(requested))return false;
        control.assertOwnership();
        return registry.onboard(requested,tenant,offsets.getFirst(),fence,boundary);
    }
    private boolean captureAndConsume(TenantStore store,SqlServerControl control,DurableBroker broker,SourceActivation activation,
                                      boolean firstActivation,TableRegistry registry) throws Exception {
        EventCodec codec=new EventCodec(mapper,tenant,activation.incarnation,registry.activationBoundaries());
        AtomicBoolean cycle=new AtomicBoolean(true), completed=new AtomicBoolean(false);
        AtomicReference<String> engineFailure=new AtomicReference<>(), consumerFailure=new AtomicReference<>();
        AtomicReference<HistoryEvent> lastEvent=new AtomicReference<>();
        AtomicReference<Instant> progress=new AtomicReference<>(Instant.now());
        Instant initializingSince=Instant.now();
        String startingOffset=store.offsets().isEmpty()?null:store.offsets().getFirst().value;
        boolean reconfigure=false;
        try(DurableBroker.Consumer consumer=broker.consumer(); DurableBroker.Publisher publisher=broker.publisher()) {
            ConfirmedChangeConsumer changes=new ConfirmedChangeConsumer(codec,publisher::publish,
                    () -> { control.assertOwnership(); progress.set(Instant.now()); }, lastEvent::set);
            engine=DebeziumEngine.create(Json.class).using(DebeziumConfiguration.create(tenant,activation.fence))
                    .notifying(changes).using((success,message,error) -> {
                        if(!success)engineFailure.set(error==null?"CAPTURE_ENGINE_FAILED":code(error));
                        completed.set(true);
                    }).build();
            Thread consumerThread=Thread.ofVirtual().name("cdc-history-"+tenant.id()).start(() -> {
                while(running.get()&&cycle.get()) {
                    try {
                        String json=consumer.receive();
                        if(json==null)continue;
                        control.assertOwnership();
                        HistoryDelivery.deliver(json,codec,event -> store.save(event,activation.fence),consumer::commit);
                        consumerFailure.set(null);
                    } catch(Exception e) {
                        consumerFailure.set(code(e));
                        try { consumer.rollback(); } catch(Exception rollback) { cycle.set(false); }
                        if(cycle.get())pause(settings.retryDelay());
                    }
                }
            });
            Thread captureThread=Thread.ofPlatform().name("cdc-capture-"+tenant.id()).start(engine);
            try {
                state="STARTING"; diagnostic="SOURCE_INITIALIZING";
                writeStatus(store,activation.fence,"STARTING","RUNNING",null,null,null);
                store.tickHeartbeat(activation.fence);
                while(running.get()&&cycle.get()&&!completed.get()) {
                    pause(settings.monitorInterval());
                    if(!running.get())break;
                    control.assertOwnership();
                    store.tickHeartbeat(activation.fence);
                    String savedLsn;
                    try { savedLsn=RecoveryGuard.savedLsn(activation,store.offsets(),tenant,mapper); }
                    catch(ContinuityException e) {
                        if(firstActivation&&Duration.between(initializingSince,Instant.now()).compareTo(Duration.ofMinutes(2))<0
                                && ("SOURCE_OFFSETS_MISSING_OR_AMBIGUOUS".equals(e.getMessage())||"SOURCE_INITIALIZATION_INCOMPLETE".equals(e.getMessage()))) {
                            state="STARTING"; diagnostic="SOURCE_INITIALIZING";
                            writeStatus(store,activation.fence,"STARTING","RUNNING",null,null,null);continue;
                        }
                        throw e;
                    }
                    List<SqlServerControl.TableObservation> observed=control.observeTables();
                    registry.observe(observed,savedLsn,activation.fence);registry.assertActiveContinuity();
                    SqlServerControl.Diagnostics info=control.inspect(savedLsn,tenant.capturedTables());
                    String currentOffset=store.offsets().getFirst().value;
                    if(offsetAdvanced(startingOffset,currentOffset))registry.confirmedProgress(activation.fence,savedLsn);
                    long queued=broker.verifyQueue();
                    String warning=info.warning();
                    if(Duration.between(progress.get(),Instant.now()).compareTo(Duration.ofMinutes(2))>0)warning="CAPTURE_PROGRESS_STALE";
                    if(queued>100000)warning="BROKER_BACKLOG_HIGH";
                    String failure=consumerFailure.get();
                    state=failure!=null?"ERROR":warning!=null?"DEGRADED":"RUNNING";
                    diagnostic=failure!=null?"CONSUMER_"+failure:warning!=null?warning:"CAPTURE_AND_CONSUMER_RUNNING";
                    writeStatus(store,activation.fence,warning==null?"RUNNING":"DEGRADED",failure==null?"RUNNING":"ERROR",lastEvent.get(),savedLsn,info);
                    store.prune(Instant.now().minus(Duration.ofDays(settings.historyRetentionDays())),settings.retentionBatchSize(),activation.fence);
                    if(!registry.readyRequests(observed,savedLsn).isEmpty()) {reconfigure=true;break;}
                }
                if(running.get()&&!reconfigure)throw new ContinuityException(engineFailure.get()!=null?engineFailure.get():"PIPELINE_STOPPED_UNEXPECTEDLY");
            } finally {
                cycle.set(false);
                try { engine.close(); } // Flushes only records confirmed by the ChangeConsumer.
                catch(Exception e) { captureThread.interrupt(); }
                join(captureThread,45000);
                consumerThread.interrupt(); join(consumerThread,35000);
                if(captureThread.isAlive()||consumerThread.isAlive()) {
                    // Do not release ownership while an old worker could still publish/checkpoint.
                    LOG.error("CDC tenant={} code=SHUTDOWN_TIMEOUT_PROCESS_EXIT",tenant.id());
                    Runtime.getRuntime().halt(2);
                }
                engine=null;
                if(!running.get()) { state="STOPPED";diagnostic="SERVICE_STOPPED";writeStatus(store,activation.fence,"STOPPED","STOPPED",lastEvent.get(),null,null); }
            }
        }
        return reconfigure;
    }
    boolean offsetAdvanced(String previous,String current) {
        if(previous==null)return current!=null;
        try {
            var before=mapper.readTree(previous);var after=mapper.readTree(current);
            int commit=Arrays.compareUnsigned(SqlServerControl.parseLsn(after.path("commit_lsn").asText()),SqlServerControl.parseLsn(before.path("commit_lsn").asText()));
            if(commit!=0)return commit>0;
            String first=before.path("change_lsn").asText(),second=after.path("change_lsn").asText();
            if("NULL".equals(first))return !"NULL".equals(second);
            if("NULL".equals(second))return false;
            int change=Arrays.compareUnsigned(SqlServerControl.parseLsn(second),SqlServerControl.parseLsn(first));
            return change>0||(change==0&&after.path("event_serial_no").asLong()>before.path("event_serial_no").asLong());
        } catch(Exception error) {return false;}
    }
    private void writeStatus(TenantStore store,long fence,String capture,String consumer,HistoryEvent event,String savedLsn,SqlServerControl.Diagnostics info) {
        updatedAt=Instant.now();
        try {
            CdcStatus status=new CdcStatus(); status.sourceId=tenant.sourceId(); status.tenantId=tenant.id();
            status.state=state; status.message=diagnostic; status.updatedAt=updatedAt;
            status.captureState=capture;status.consumerState=consumer;
            status.lastEventAt=event==null?null:event.occurredAt;status.lastCommitLsn=savedLsn;
            if(info!=null) {status.retentionMinutes=info.retentionMinutes();status.retentionHeadroomSeconds=info.headroomSeconds();}
            store.status(status,fence);
        } catch(Exception e) { state="ERROR";diagnostic="STATUS_WRITE_FAILED"; }
    }
    static String code(Throwable e) {
        if(e instanceof ContinuityException)return e.getMessage();
        StringBuilder code=new StringBuilder("DEPENDENCY_FAILURE");
        java.util.Set<Throwable> visited=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        for(Throwable cause=e;cause!=null&&visited.size()<6&&visited.add(cause);cause=cause.getCause()) {
            code.append('_').append(cause.getClass().getSimpleName());
            if(cause instanceof java.sql.SQLException sql) {
                String state=sql.getSQLState();
                if(state!=null&&state.matches("[A-Za-z0-9]{5}"))code.append("_SQLSTATE_").append(state);
                code.append("_SQLCODE_").append(sql.getErrorCode());
            }
        }
        return code.toString();
    }
    private void pause(Duration duration) {
        try { Thread.sleep(duration); } catch(InterruptedException e) { /* running/cycle flags decide shutdown. */ }
    }
    private void join(Thread thread,long millis) {
        long deadline=System.nanoTime()+millis*1_000_000;
        while(thread.isAlive()&&System.nanoTime()<deadline) {
            try { thread.join(Math.max(1,(deadline-System.nanoTime())/1_000_000)); }
            catch(InterruptedException e) { /* Keep ownership until worker termination is known. */ }
        }
    }
    public boolean healthy() { return "RUNNING".equals(state)&&updatedAt.isAfter(Instant.now().minus(settings.monitorInterval().multipliedBy(3))); }
    public String state() { return state; }
    public String diagnostic() { return diagnostic; }
    @Override public void close() {
        running.set(false);
        Thread thread=worker;
        if(thread!=null) {
            thread.interrupt();
            try { thread.join(55000); } catch(InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }
}
