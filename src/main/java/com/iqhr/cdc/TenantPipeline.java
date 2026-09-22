package com.iqhr.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iqhr.cdc.broker.*;
import com.iqhr.cdc.model.*;
import com.iqhr.cdc.source.*;
import com.iqhr.cdc.store.*;
import io.debezium.engine.*;
import io.debezium.engine.format.Json;
import java.time.*;
import java.util.concurrent.atomic.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A failed tenant never stops another tenant. All durable progress survives process restarts. */
public final class TenantPipeline implements AutoCloseable {
    private static final Logger LOG=LoggerFactory.getLogger(TenantPipeline.class);
    private final CdcProperties settings;
    private final CdcProperties.Tenant tenant;
    private final ObjectMapper mapper;
    private final AtomicBoolean running=new AtomicBoolean(true);
    private volatile String state="STARTING", diagnostic="INITIALIZING";
    private volatile Instant updatedAt=Instant.now();
    private volatile Thread worker;
    private volatile DebeziumEngine<ChangeEvent<String,String>> engine;
    public TenantPipeline(CdcProperties settings,CdcProperties.Tenant tenant,ObjectMapper mapper) {
        this.settings=settings; this.tenant=tenant; this.mapper=mapper;
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
        try(SqlServerControl control=new SqlServerControl(tenant); TenantStore store=TenantStore.open(tenant);
            DurableBroker broker=new DurableBroker(settings.broker(),tenant)) {
            SourceActivation previous=store.activation();
            long fence=0;
            try {
                String savedLsn=RecoveryGuard.savedLsn(previous,store.offsets(),tenant,mapper);
                if(previous!=null&&!store.hasSchemaHistory())throw new ContinuityException("SCHEMA_HISTORY_MISSING");
                if(previous==null&&(store.hasHistory()||store.hasSchemaHistory()))throw new ContinuityException("ACTIVATION_STATE_INCONSISTENT");
                SqlServerControl.Diagnostics initial=control.inspect(savedLsn);
                if(initial.retentionMinutes()<tenant.minimumRetentionMinutes())throw new ContinuityException("CDC_RETENTION_BELOW_14_DAYS");
                broker.verifyQueue(); // MUST precede the first activation and engine start.
                SourceActivation activation=store.claim(); fence=activation.fence;
                captureAndConsume(store,control,broker,activation,previous==null);
            } catch(Exception e) {
                state="ERROR"; diagnostic=code(e); updatedAt=Instant.now();
                // Never overwrite another live owner's status before having acquired the session lock.
                writeStatus(store,fence,"ERROR","ERROR",null,null,null);
                throw e;
            }
        }
    }
    private void captureAndConsume(TenantStore store,SqlServerControl control,DurableBroker broker,SourceActivation activation,boolean firstActivation) throws Exception {
        EventCodec codec=new EventCodec(mapper,tenant,activation.incarnation);
        AtomicBoolean cycle=new AtomicBoolean(true), completed=new AtomicBoolean(false);
        AtomicReference<String> engineFailure=new AtomicReference<>(), consumerFailure=new AtomicReference<>();
        AtomicReference<HistoryEvent> lastEvent=new AtomicReference<>();
        AtomicReference<Instant> progress=new AtomicReference<>(Instant.now());
        Instant initializingSince=Instant.now();
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
                    SqlServerControl.Diagnostics info=control.inspect(savedLsn);
                    long queued=broker.verifyQueue();
                    String warning=info.warning();
                    if(Duration.between(progress.get(),Instant.now()).compareTo(Duration.ofMinutes(2))>0)warning="CAPTURE_PROGRESS_STALE";
                    if(queued>100000)warning="BROKER_BACKLOG_HIGH";
                    String failure=consumerFailure.get();
                    state=failure!=null?"ERROR":warning!=null?"DEGRADED":"RUNNING";
                    diagnostic=failure!=null?"CONSUMER_"+failure:warning!=null?warning:"CAPTURE_AND_CONSUMER_RUNNING";
                    writeStatus(store,activation.fence,warning==null?"RUNNING":"DEGRADED",failure==null?"RUNNING":"ERROR",lastEvent.get(),savedLsn,info);
                    store.prune(Instant.now().minus(Duration.ofDays(settings.historyRetentionDays())),settings.retentionBatchSize(),activation.fence);
                }
                if(running.get())throw new ContinuityException(engineFailure.get()!=null?engineFailure.get():"PIPELINE_STOPPED_UNEXPECTEDLY");
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
