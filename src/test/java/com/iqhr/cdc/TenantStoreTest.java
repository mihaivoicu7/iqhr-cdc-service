package com.iqhr.cdc;

import static org.assertj.core.api.Assertions.*;
import com.iqhr.cdc.store.*;
import com.iqhr.cdc.model.*;
import java.time.*;
import java.util.UUID;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

class TenantStoreTest {
    @Test void actualJpaCommitDeduplicatesAcrossFactoriesAndFencesOldOwner() {
        JdbcDataSource ds=new JdbcDataSource();ds.setURL("jdbc:h2:mem:"+UUID.randomUUID()+";MODE=MSSQLServer;DB_CLOSE_DELAY=-1;INIT=CREATE SCHEMA IF NOT EXISTS dbo");
        long originalFence;
        try(var store=new TenantStore(Fixtures.TENANT,TenantStore.factory(ds,"org.hibernate.dialect.H2Dialect","create-only"))) {
            var activation=store.claim();originalFence=activation.fence;
            store.tickHeartbeat(activation.fence);store.tickHeartbeat(activation.fence);
            long tickSequence=store.transaction(em -> em.find(SourceHeartbeat.class,1).tickSequence);
            assertThat(tickSequence).isEqualTo(2L);
            assertThat(store.save(Fixtures.event(),activation.fence)).isTrue();
            assertThat(store.save(Fixtures.event(),activation.fence)).isFalse();
        }
        try(var restarted=new TenantStore(Fixtures.TENANT,TenantStore.factory(ds,"org.hibernate.dialect.H2Dialect","none"))) {
            var owner=restarted.claim();assertThat(owner.fence).isGreaterThan(originalFence);
            assertThat(restarted.save(Fixtures.event(),owner.fence)).isFalse();
            assertThatThrownBy(() -> restarted.save(Fixtures.event(),originalFence)).hasMessage("SOURCE_FENCED");
            var foreign=Fixtures.event();foreign.tenantId="OTHER";
            assertThatThrownBy(() -> restarted.save(foreign,owner.fence)).hasMessage("MESSAGE_TENANT_MISMATCH");
            assertThat(restarted.prune(Instant.parse("2026-01-01T00:00:00Z"),1,owner.fence)).isEqualTo(1);
            assertThat(restarted.prune(Instant.parse("2026-01-01T00:00:00Z"),1,owner.fence)).isZero();
        }
    }
    @Test void configurationsRejectSharedDatabaseAndServiceOwnedCapture() {
        assertThatThrownBy(() -> new CdcProperties(new CdcProperties.Broker("vm://0","u","p"),java.util.List.of(Fixtures.TENANT,Fixtures.tenant("OTHER")),null,null,365,500))
                .hasMessageContaining("physical database");
        assertThatThrownBy(() -> new CdcProperties.Tenant("A","s","localhost",1433,"db","u","p",false,true,
                java.util.List.of("dbo.IQHR_CdcEvent"),true,20160,1024,128)).hasMessageContaining("service-owned");
    }
}
