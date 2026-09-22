package com.iqhr.cdc.store;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import org.junit.jupiter.api.Test;

class TenantStoreLifecycleTest {
    @Test void startupFailureClosesJpaFactoryAndOwnedPool() {
        EntityManagerFactory emf=mock(EntityManagerFactory.class);
        HikariDataSource pool=mock(HikariDataSource.class);
        assertThatThrownBy(() -> {
            try(TenantStore ignored=new TenantStore(null,emf,pool)) {
                throw new ContinuityException("SOURCE_REPLAY_COORDINATES_INVALID");
            }
        }).hasMessage("SOURCE_REPLAY_COORDINATES_INVALID");
        verify(emf).close();verify(pool).close();
    }
    @Test void jpaCloseFailureCannotLeakTheOwnedPool() {
        EntityManagerFactory emf=mock(EntityManagerFactory.class);
        HikariDataSource pool=mock(HikariDataSource.class);
        doThrow(new IllegalStateException("close failed")).when(emf).close();
        assertThatThrownBy(() -> new TenantStore(null,emf,pool).close()).hasMessage("close failed");
        verify(pool).close();
    }
}
