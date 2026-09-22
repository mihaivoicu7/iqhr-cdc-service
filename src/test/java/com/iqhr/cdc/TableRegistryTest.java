package com.iqhr.cdc;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.iqhr.cdc.model.*;
import com.iqhr.cdc.source.*;
import com.iqhr.cdc.store.*;
import jakarta.persistence.LockModeType;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

class TableRegistryTest {
    static final String SAVED="00000027:00000ac0:0003";
    static final String ACTIVATION="00000027:00000af0:0003";
    static final String MIN="00000027:00000a00:0001";
    static final String NEW_TABLE="HR_EmployeeAllocation";
    static final String NEW_NAME="dbo."+NEW_TABLE;
    static SqlServerControl.TableObservation observed(String table,String fingerprint,String minimum) {
        return new SqlServerControl.TableObservation("dbo",table,fingerprint,true,null,List.of(SqlServerControl.parseLsn(minimum)));
    }
    static List<SqlServerControl.TableObservation> inventory() {
        return List.of(observed("HR_EmployeeContractInfo","original-generation",MIN),observed(NEW_TABLE,"new-generation",MIN));
    }
    static JdbcDataSource database() {
        var ds=new JdbcDataSource();ds.setURL("jdbc:h2:mem:"+UUID.randomUUID()+";MODE=MSSQLServer;DB_CLOSE_DELAY=-1;INIT=CREATE SCHEMA IF NOT EXISTS dbo");return ds;
    }
    static TenantStore store(JdbcDataSource ds,String ddl) {
        return new TenantStore(Fixtures.TENANT,TenantStore.factory(ds,"org.hibernate.dialect.H2Dialect",ddl));
    }
    static OffsetRow seed(TenantStore store) {
        OffsetRow offset=new OffsetRow();offset.id="durable-offset";
        offset.key="[\""+Fixtures.TENANT.connectorName()+"\",{\"server\":\""+Fixtures.TENANT.connectorName()+"\",\"database\":\"tdev_technophar\"}]";
        offset.value="{\"commit_lsn\":\""+SAVED+"\",\"change_lsn\":\"00000027:00000ab8:0002\",\"event_serial_no\":2,\"extra\":true}";
        store.transaction(em -> {em.persist(offset);SchemaHistoryRow schema=new SchemaHistoryRow();schema.sequence=1;em.persist(schema);em.persist(Fixtures.event());return null;});
        return offset;
    }
    static void request(TenantStore store) {
        store.transaction(em -> {CdcTable row=em.find(CdcTable.class,TableRegistry.id(Fixtures.TENANT,"dbo",NEW_TABLE),LockModeType.PESSIMISTIC_WRITE);
            row.desiredEnabled=true;row.state="PENDING";row.requestedAt=Instant.parse("2026-09-22T14:00:00Z");row.requestedBy="operator";return null;});
    }
    static CdcTable row(TenantStore store,String table) {return store.transaction(em -> em.find(CdcTable.class,TableRegistry.id(Fixtures.TENANT,"dbo",table)));}

    @Test void onboardingCommitsRegistryHashAndFenceThenRestartUsesManifestWithoutRewinding() {
        var ds=database();String incarnation;String originalOffset;long originalFence;
        try(var store=store(ds,"create-only")) {
            var activation=store.claim();incarnation=activation.incarnation;originalFence=activation.fence;
            OffsetRow offset=seed(store);originalOffset=offset.value;
            var registry=new TableRegistry(store,Fixtures.TENANT);registry.initialize(inventory());registry.observe(inventory(),SAVED,activation.fence);
            assertThat(row(store,NEW_TABLE).state).isEqualTo("DISCOVERED");
            assertThat(registry.hasUnconfiguredTables()).isTrue();
            assertThat(registry.effectiveTenant().tables()).containsExactly("dbo.HR_EmployeeContractInfo");
            request(store);
            assertThat(registry.readyRequests(inventory(),SAVED)).containsExactly(NEW_NAME);
            // A later validation failure rolls back the already-mutated managed registry row.
            assertThatThrownBy(() -> registry.onboard(List.of(NEW_NAME,"dbo.Missing"),Fixtures.TENANT,offset,activation.fence,ACTIVATION))
                    .hasMessage("ONBOARDING_REQUEST_CHANGED");
            assertThat(row(store,NEW_TABLE).active).isFalse();assertThat(store.activation().fence).isEqualTo(originalFence);
            assertThat(registry.onboard(List.of(NEW_NAME),Fixtures.TENANT,offset,activation.fence,ACTIVATION)).isTrue();
            var pending=row(store,NEW_TABLE);
            assertThat(pending.active).isTrue();assertThat(pending.state).isEqualTo("ONBOARDING");
            assertThat(pending.activationLsn).isEqualTo(ACTIVATION);assertThat(pending.enabledAt).isNull();
            assertThat(store.activation().fence).isEqualTo(originalFence+1);
            assertThat(store.offsets().getFirst().value).isEqualTo(originalOffset);
            assertThat(store.hasSchemaHistory()).isTrue();assertThat(store.hasHistory()).isTrue();
        }
        // Crash after configuration commit, before new engine startup: the old environment list is still safe.
        try(var restarted=store(ds,"none")) {
            var registry=new TableRegistry(restarted,Fixtures.TENANT);registry.initialize(inventory());
            var effective=registry.effectiveTenant();
            assertThat(effective.tables()).containsExactly(NEW_NAME,"dbo.HR_EmployeeContractInfo");
            assertThat(restarted.activation().configurationHash).isEqualTo(TenantStore.configurationHash(effective));
            assertThat(restarted.activation().incarnation).isEqualTo(incarnation);
            assertThat(restarted.offsets().getFirst().value).isEqualTo(originalOffset);
            assertThat(RecoveryGuard.savedLsn(restarted.activation(),restarted.offsets(),effective,Fixtures.MAPPER)).isEqualTo(SAVED);
            var owner=restarted.claim(effective);registry.starting(owner.fence);
            assertThat(row(restarted,NEW_TABLE).state).isEqualTo("ONBOARDING");
            registry.confirmedProgress(owner.fence,SAVED);
            assertThat(row(restarted,NEW_TABLE).state).isEqualTo("ONBOARDING");
            registry.confirmedProgress(owner.fence,ACTIVATION);
            assertThat(row(restarted,NEW_TABLE).state).isEqualTo("ONBOARDING");
            registry.confirmedProgress(owner.fence,"00000027:00000af0:0004");
            assertThat(row(restarted,NEW_TABLE).state).isEqualTo("ACTIVE");
            assertThat(row(restarted,NEW_TABLE).enabledAt).isNotNull();
            assertThat(row(restarted,NEW_TABLE).requestedBy).isEqualTo("operator");
            assertThat(registry.activationBoundaries()).containsEntry(NEW_NAME,ACTIVATION);
        }
    }
    @Test void freshObservationWaitsForAdminLockAndPreservesItsRequest() throws Exception {
        try(var store=store(database(),"create-only")) {
            var owner=store.claim();var registry=new TableRegistry(store,Fixtures.TENANT);registry.initialize(inventory());registry.observe(inventory(),SAVED,owner.fence);
            long oldVersion=row(store,NEW_TABLE).version;
            CountDownLatch locked=new CountDownLatch(1),release=new CountDownLatch(1);
            var admin=CompletableFuture.runAsync(() -> store.transaction(em -> {
                CdcTable row=em.find(CdcTable.class,TableRegistry.id(Fixtures.TENANT,"dbo",NEW_TABLE),LockModeType.PESSIMISTIC_WRITE);
                row.desiredEnabled=true;row.state="PENDING";row.requestedBy="concurrent-admin";row.requestedAt=Instant.now();locked.countDown();
                try {if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("admin release timed out");}catch(InterruptedException error){throw new AssertionError(error);}return null;
            }));
            assertThat(locked.await(5,TimeUnit.SECONDS)).isTrue();
            var observer=CompletableFuture.runAsync(() -> registry.observe(inventory(),SAVED,owner.fence));
            try {assertThatThrownBy(() -> observer.get(100,TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);}
            finally {release.countDown();}
            admin.get(5,TimeUnit.SECONDS);observer.get(5,TimeUnit.SECONDS);
            var after=row(store,NEW_TABLE);
            assertThat(after.desiredEnabled).isTrue();assertThat(after.state).isEqualTo("PENDING");
            assertThat(after.requestedBy).isEqualTo("concurrent-admin");assertThat(after.requestedAt).isNotNull();assertThat(after.version).isGreaterThan(oldVersion);
        }
    }
    @Test void missingOrReplacedCaptureIsVisibleAndNeverAcceptedAsAHealthyNewGeneration() {
        try(var store=store(database(),"create-only")) {
            var owner=store.claim();var registry=new TableRegistry(store,Fixtures.TENANT);registry.initialize(inventory());registry.observe(inventory(),SAVED,owner.fence);
            registry.observe(List.of(observed(NEW_TABLE,"new-generation",MIN)),SAVED,owner.fence);
            assertThat(row(store,"HR_EmployeeContractInfo").state).isEqualTo("MISSING");
            assertThatThrownBy(registry::assertActiveContinuity).hasMessage("SOURCE_TABLE_NOT_CDC_ENABLED");
            registry.observe(List.of(observed("HR_EmployeeContractInfo","replacement-generation",MIN)),SAVED,owner.fence);
            var changed=row(store,"HR_EmployeeContractInfo");
            assertThat(changed.state).isEqualTo("BLOCKED");assertThat(changed.captureFingerprint).isEqualTo("original-generation");
            assertThatThrownBy(registry::assertActiveContinuity).hasMessage("CAPTURE_GENERATION_CHANGED");
            registry.observe(inventory(),SAVED,owner.fence);
            assertThat(row(store,"HR_EmployeeContractInfo").state).isEqualTo("BLOCKED");
        }
    }
    @Test void retentionGapIsLatchedAndPendingFutureMinimumWaits() {
        try(var store=store(database(),"create-only")) {
            var owner=store.claim();var registry=new TableRegistry(store,Fixtures.TENANT);registry.initialize(inventory());registry.observe(inventory(),SAVED,owner.fence);request(store);
            var future=List.of(observed("HR_EmployeeContractInfo","original-generation",MIN),observed(NEW_TABLE,"new-generation","00000028:00000000:0001"));
            registry.observe(future,SAVED,owner.fence);
            assertThat(registry.readyRequests(future,SAVED)).isEmpty();assertThat(row(store,NEW_TABLE).state).isEqualTo("PENDING");
            registry.observe(List.of(observed("HR_EmployeeContractInfo","original-generation","00000028:00000000:0001")),SAVED,owner.fence);
            assertThatThrownBy(registry::assertActiveContinuity).hasMessage("SOURCE_POSITION_UNAVAILABLE");
            registry.observe(inventory(),SAVED,owner.fence);assertThat(row(store,"HR_EmployeeContractInfo").state).isEqualTo("BLOCKED");
        }
    }
    @Test void staleCheckpointOrOwnerCannotCommitOnboarding() {
        try(var store=store(database(),"create-only")) {
            var owner=store.claim();var offset=seed(store);var registry=new TableRegistry(store,Fixtures.TENANT);
            registry.initialize(inventory());registry.observe(inventory(),SAVED,owner.fence);request(store);
            assertThatThrownBy(() -> registry.onboard(List.of(NEW_NAME),Fixtures.TENANT,offset,owner.fence+1,ACTIVATION)).hasMessage("SOURCE_FENCED");
            offset.value="changed";
            assertThatThrownBy(() -> registry.onboard(List.of(NEW_NAME),Fixtures.TENANT,offset,owner.fence,ACTIVATION)).hasMessage("ONBOARDING_CHECKPOINT_CHANGED");
            assertThat(row(store,NEW_TABLE).active).isFalse();
        }
    }
    @Test void establishedRegistryCannotBeSilentlyBootstrappedAfterLoss() {
        try(var store=store(database(),"create-only")) {
            store.claim();var registry=new TableRegistry(store,Fixtures.TENANT);registry.initialize(inventory());
            assertThat(store.activation().tableRegistryInitialized).isTrue();
            store.transaction(em -> {em.createQuery("from CdcTable",CdcTable.class).getResultList().forEach(em::remove);return null;});
            assertThatThrownBy(() -> registry.initialize(inventory())).hasMessage("TABLE_REGISTRY_MISSING");
        }
    }
    @Test void freshTenantMarksRegistryInTheFirstActivationTransaction() {
        try(var store=store(database(),"create-only")) {
            var registry=new TableRegistry(store,Fixtures.TENANT);registry.initialize(inventory());
            assertThat(store.activation()).isNull();
            var claimed=store.claim(registry.effectiveTenant());
            assertThat(claimed.tableRegistryInitialized).isTrue();
            store.transaction(em -> {em.createQuery("from CdcTable",CdcTable.class).getResultList().forEach(em::remove);return null;});
            assertThatThrownBy(() -> registry.initialize(inventory())).hasMessage("TABLE_REGISTRY_MISSING");
        }
    }
    @Test void stoppedReaderOnboardsWithCapturedHeartbeatBoundaryBeforeAnyEngineAdvance() throws Exception {
        try(var store=store(database(),"create-only")) {
            var owner=store.claim();var checkpoint=seed(store);var registry=new TableRegistry(store,Fixtures.TENANT);
            registry.initialize(inventory());registry.observe(inventory(),SAVED,owner.fence);request(store);
            var control=mock(SqlServerControl.class);
            when(control.boundaryHasNoNewRows(anyCollection(),eq(SAVED),anyString())).thenReturn(true);
            when(control.onboardingBoundary(anyLong())).thenAnswer(call -> {
                assertThat(row(store,NEW_TABLE).active).isFalse();
                assertThat(store.offsets().getFirst().value).isEqualTo(checkpoint.value);
                return ACTIVATION;
            });
            when(control.observeTables()).thenReturn(inventory());
            var pipeline=new TenantPipeline(new CdcProperties(new CdcProperties.Broker("vm://0","u","p"),List.of(Fixtures.TENANT),null,null,365,500),Fixtures.TENANT,Fixtures.MAPPER);
            assertThat(pipeline.tryOnboard(store,control,registry,inventory(),owner.fence)).isTrue();
            assertThat(row(store,NEW_TABLE).activationLsn).isEqualTo(ACTIVATION);
            assertThat(store.offsets().getFirst().value).isEqualTo(checkpoint.value);
            var order=inOrder(control);
            order.verify(control).boundaryHasNoNewRows(anyCollection(),eq(SAVED),anyString());
            order.verify(control).onboardingBoundary(anyLong());
            order.verify(control).inspect(eq(ACTIVATION),anyCollection());
            order.verify(control).observeTables();
        }
    }
    @Test void boundaryCollisionLeavesRequestPendingAndDoesNotCreateBarrier() throws Exception {
        try(var store=store(database(),"create-only")) {
            var owner=store.claim();seed(store);var registry=new TableRegistry(store,Fixtures.TENANT);
            registry.initialize(inventory());registry.observe(inventory(),SAVED,owner.fence);request(store);
            var control=mock(SqlServerControl.class);when(control.boundaryHasNoNewRows(anyCollection(),anyString(),anyString())).thenReturn(false);
            var pipeline=new TenantPipeline(new CdcProperties(new CdcProperties.Broker("vm://0","u","p"),List.of(Fixtures.TENANT),null,null,365,500),Fixtures.TENANT,Fixtures.MAPPER);
            assertThat(pipeline.tryOnboard(store,control,registry,inventory(),owner.fence)).isFalse();
            assertThat(row(store,NEW_TABLE).state).isEqualTo("PENDING");assertThat(row(store,NEW_TABLE).active).isFalse();
            verify(control,never()).onboardingBoundary(anyLong());
        }
    }
}
