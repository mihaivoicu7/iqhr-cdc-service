package com.iqhr.cdc.store;

import com.iqhr.cdc.CdcProperties.Tenant;
import com.iqhr.cdc.model.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;

/** One factory per tenant: no thread-local routing can accidentally select another database. */
public class TenantStore implements AutoCloseable {
    private final EntityManagerFactory emf;
    private final Tenant tenant;
    private final HikariDataSource ownedDataSource;

    public static TenantStore open(Tenant tenant) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(tenant.jdbcUrl()); config.setUsername(tenant.user()); config.setPassword(tenant.password());
        config.setMaximumPoolSize(4); config.setMinimumIdle(0); config.setConnectionTimeout(15000);
        config.setPoolName("cdc-" + tenant.id());
        HikariDataSource ds = new HikariDataSource(config);
        try {
            Flyway.configure().dataSource(ds).locations("classpath:db/migration").table("IQHR_CdcFlywayHistory")
                    .defaultSchema("dbo").baselineOnMigrate(true).baselineVersion("0").load().migrate();
            return new TenantStore(tenant, factory(ds, "org.hibernate.dialect.SQLServerDialect", "none"), ds);
        } catch (RuntimeException e) { ds.close(); throw e; }
    }
    public TenantStore(Tenant tenant, EntityManagerFactory emf) { this(tenant, emf, null); }
    TenantStore(Tenant tenant, EntityManagerFactory emf, HikariDataSource ds) {
        this.tenant=tenant; this.emf=emf; ownedDataSource=ds;
    }
    public static EntityManagerFactory factory(DataSource ds, String dialect, String ddl) {
        LocalContainerEntityManagerFactoryBean bean = new LocalContainerEntityManagerFactoryBean();
        bean.setDataSource(ds); bean.setPackagesToScan("com.iqhr.cdc.model"); bean.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
        bean.setJpaPropertyMap(Map.of("hibernate.dialect",dialect,"hibernate.hbm2ddl.auto",ddl,
                "hibernate.jdbc.time_zone","UTC", "hibernate.show_sql","false", "hibernate.jdbc.batch_size","50"));
        bean.afterPropertiesSet(); return Objects.requireNonNull(bean.getObject());
    }
    public <T> T transaction(Function<EntityManager,T> work) {
        try (EntityManager em=emf.createEntityManager()) {
            EntityTransaction tx=em.getTransaction(); tx.begin();
            try { T result=work.apply(em); tx.commit(); return result; }
            catch (RuntimeException e) { if(tx.isActive())tx.rollback(); throw e; }
        }
    }
    public SourceActivation activation() {
        return transaction(em -> {
            var rows=em.createQuery("from SourceActivation",SourceActivation.class).setMaxResults(2).getResultList();
            if(rows.isEmpty())return null;
            if(rows.size()!=1||!tenant.sourceId().equals(rows.getFirst().sourceId))throw new ContinuityException("SOURCE_IDENTITY_CHANGED");
            return rows.getFirst();
        });
    }
    public boolean hasHistory() { return transaction(em -> !em.createQuery("select e.eventId from HistoryEvent e",String.class).setMaxResults(1).getResultList().isEmpty()); }
    public boolean hasSchemaHistory() { return transaction(em -> !em.createQuery("select e.sequence from SchemaHistoryRow e",Long.class).setMaxResults(1).getResultList().isEmpty()); }
    public List<OffsetRow> offsets() { return transaction(em -> em.createQuery("from OffsetRow",OffsetRow.class).getResultList()); }
    public SourceActivation claim() {
        return claim(tenant);
    }
    public SourceActivation claim(Tenant effectiveTenant) {
        if(!tenant.id().equals(effectiveTenant.id())||!tenant.sourceId().equals(effectiveTenant.sourceId()))
            throw new ContinuityException("SOURCE_IDENTITY_CHANGED");
        return transaction(em -> {
            SourceActivation row=em.find(SourceActivation.class,tenant.sourceId(),LockModeType.PESSIMISTIC_WRITE);
            String hash=configurationHash(effectiveTenant);
            if(row==null) {
                row=new SourceActivation(); row.sourceId=tenant.sourceId(); row.tenantId=tenant.id();
                row.incarnation=UUID.randomUUID().toString(); row.configurationHash=hash; row.createdAt=Instant.now(); row.fence=1;
                em.persist(row);
            } else {
                if(!hash.equals(row.configurationHash) || !tenant.id().equals(row.tenantId))
                    throw new ContinuityException("SOURCE_CONFIGURATION_CHANGED");
                row.fence++;
            }
            // Fresh tenants initialize the registry before claiming their first activation.
            // Commit its existence marker with that activation, without a crash window between writes.
            if(!em.createQuery("select t.tableId from CdcTable t where t.tenantId=:tenant and t.sourceId=:source",String.class)
                    .setParameter("tenant",tenant.id()).setParameter("source",tenant.sourceId()).setMaxResults(1).getResultList().isEmpty())
                row.tableRegistryInitialized=true;
            return row;
        });
    }
    public void assertFence(EntityManager em, long fence) {
        SourceActivation row=em.find(SourceActivation.class,tenant.sourceId(),LockModeType.PESSIMISTIC_READ);
        if(row==null || row.fence!=fence)throw new ContinuityException("SOURCE_FENCED");
    }
    public boolean save(HistoryEvent event, long fence) {
        if(!tenant.id().equals(event.tenantId)||!tenant.sourceId().equals(event.sourceId))
            throw new ContinuityException("MESSAGE_TENANT_MISMATCH");
        return transaction(em -> {
            assertFence(em,fence);
            if(em.find(HistoryEvent.class,event.eventId)!=null)return false;
            em.persist(event);
            return true;
        });
    }
    public void status(CdcStatus status, long fence) {
        transaction(em -> {
            if(fence>0)assertFence(em,fence);
            CdcStatus previous=em.find(CdcStatus.class,status.sourceId);
            if(previous!=null) {
                if(status.lastEventAt==null || (previous.lastEventAt!=null&&previous.lastEventAt.isAfter(status.lastEventAt)))status.lastEventAt=previous.lastEventAt;
                if(status.lastCommitLsn==null)status.lastCommitLsn=previous.lastCommitLsn;
                if(status.retentionMinutes==null)status.retentionMinutes=previous.retentionMinutes;
                if(status.retentionHeadroomSeconds==null)status.retentionHeadroomSeconds=previous.retentionHeadroomSeconds;
            }
            em.merge(status);return null;
        });
    }
    public int prune(Instant before, int limit, long fence) {
        return transaction(em -> {
            assertFence(em,fence);
            List<HistoryEvent> rows=em.createQuery("from HistoryEvent e where e.tenantId=:tenant and e.occurredAt<:before order by e.occurredAt,e.eventId",HistoryEvent.class)
                    .setParameter("tenant",tenant.id()).setParameter("before",before).setMaxResults(limit).getResultList();
            rows.forEach(em::remove); return rows.size();
        });
    }
    public long tickHeartbeat(long fence) {
        return transaction(em -> {
            assertFence(em,fence);
            SourceHeartbeat row=em.find(SourceHeartbeat.class,1,LockModeType.PESSIMISTIC_WRITE);
            boolean fresh=row==null;
            if(fresh) {row=new SourceHeartbeat();row.singletonId=1;}
            row.tickSequence++;row.tickedAt=Instant.now();
            if(fresh)em.persist(row);
            return row.tickSequence;
        });
    }
    public static String sha256(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch(java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
    public static String configurationHash(Tenant tenant) {
        return sha256(tenant.id()+"\n"+tenant.sourceId()+"\n"+tenant.host()+":"+tenant.port()+"\n"+tenant.database()+"\n"+String.join(",",new TreeSet<>(tenant.capturedTables())));
    }
    @Override public void close() {
        try { emf.close(); }
        finally { if(ownedDataSource!=null)ownedDataSource.close(); }
    }
}
