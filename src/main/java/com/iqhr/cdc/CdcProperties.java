package com.iqhr.cdc;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("cdc")
public record CdcProperties(Broker broker, List<Tenant> tenants, Duration monitorInterval,
                            Duration retryDelay, int historyRetentionDays, int retentionBatchSize) {
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    public static final String HEARTBEAT_TABLE="dbo.IQHR_CdcHeartbeat";
    public CdcProperties {
        tenants = tenants == null ? List.of() : List.copyOf(tenants);
        monitorInterval = monitorInterval == null ? Duration.ofSeconds(30) : monitorInterval;
        retryDelay = retryDelay == null ? Duration.ofSeconds(10) : retryDelay;
        historyRetentionDays = historyRetentionDays == 0 ? 365 : historyRetentionDays;
        retentionBatchSize = retentionBatchSize == 0 ? 500 : retentionBatchSize;
        if (monitorInterval.compareTo(Duration.ofSeconds(5)) < 0 || monitorInterval.compareTo(Duration.ofSeconds(30)) > 0 || retryDelay.isNegative()
                || retryDelay.isZero() || historyRetentionDays != 365 || retentionBatchSize < 1 || retentionBatchSize > 2000)
            throw new IllegalArgumentException("Invalid CDC timing or retention configuration");
        Set<String> ids = new HashSet<>();
        Set<String> databases = new HashSet<>();
        for (Tenant tenant : tenants) {
            if (!ids.add(tenant.id()) || !databases.add((tenant.host() + ":" + tenant.port() + "/" + tenant.database()).toLowerCase()))
                throw new IllegalArgumentException("Each tenant and physical database must have one configured pipeline");
        }
        if (!tenants.isEmpty() && broker == null) throw new IllegalArgumentException("Broker configuration required");
    }
    public record Broker(String url, String user, String password) {
        public Broker {
            if (url == null || !url.matches("(?:tcp|vm)://.*") || user == null || password == null)
                throw new IllegalArgumentException("Broker URL and credentials required");
        }
        @Override public String toString() { return "Broker[credentials redacted]"; }
    }
    public record Tenant(String id, String sourceId, String host, int port, String database, String user,
                         String password, boolean encrypt, boolean trustServerCertificate, List<String> tables,
                         boolean enabled, int minimumRetentionMinutes, int maxQueueSize, int maxBatchSize) {
        public Tenant {
            if (id == null || !ID.matcher(id).matches() || sourceId == null || !ID.matcher(sourceId).matches())
                throw new IllegalArgumentException("Tenant/source identifiers must use letters, numbers, underscore or hyphen");
            if (host == null || !host.matches("[A-Za-z0-9.-]+") || database == null || !IDENTIFIER.matcher(database).matches()
                    || user == null || password == null) throw new IllegalArgumentException("Invalid SQL Server configuration");
            port = port == 0 ? 1433 : port;
            if (port < 1 || port > 65535) throw new IllegalArgumentException("Invalid SQL Server port");
            tables = tables == null ? List.of() : List.copyOf(tables);
            if (tables.isEmpty()) throw new IllegalArgumentException("Explicit source table allowlist required");
            for (String table : tables) {
                if (!table.matches("[A-Za-z_][A-Za-z0-9_]*\\.[A-Za-z_][A-Za-z0-9_]*")
                        || table.toLowerCase().contains("iqhr_cdc")) throw new IllegalArgumentException("Invalid or service-owned source table");
            }
            minimumRetentionMinutes = minimumRetentionMinutes == 0 ? 20160 : minimumRetentionMinutes;
            if (minimumRetentionMinutes < 20160) throw new IllegalArgumentException("Fourteen-day CDC retention required");
            maxQueueSize = maxQueueSize == 0 ? 1024 : maxQueueSize;
            maxBatchSize = maxBatchSize == 0 ? 128 : maxBatchSize;
            if (maxQueueSize <= maxBatchSize || maxQueueSize > 8192 || maxBatchSize < 1)
                throw new IllegalArgumentException("Invalid bounded capture queue/batch");
        }
        public String jdbcUrl() {
            return "jdbc:sqlserver://" + host + ":" + port + ";databaseName=" + database + ";encrypt=" + encrypt
                    + ";trustServerCertificate=" + trustServerCertificate + ";loginTimeout=10;socketTimeout=30000;applicationName=IQHR-CDC";
        }
        public String address() { return "iqhr.cdc." + id; }
        public List<String> capturedTables() {
            return java.util.stream.Stream.concat(tables.stream(),java.util.stream.Stream.of(HEARTBEAT_TABLE)).distinct().toList();
        }
        public String queue() { return address() + ".history"; }
        public String connectorName() { return "iqhr-cdc-" + id + "-" + sourceId; }
        public Tenant withTables(List<String> effectiveTables) {
            return new Tenant(id,sourceId,host,port,database,user,password,encrypt,trustServerCertificate,
                    effectiveTables,enabled,minimumRetentionMinutes,maxQueueSize,maxBatchSize);
        }
        @Override public String toString() { return "Tenant[id=" + id + ", sourceId=" + sourceId + "]"; }
    }
}
