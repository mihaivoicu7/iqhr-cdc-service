package com.iqhr.cdc.source;

import com.iqhr.cdc.CdcProperties.Tenant;
import java.util.Properties;
import java.util.regex.Pattern;

public final class DebeziumConfiguration {
    private DebeziumConfiguration() {}
    public static Properties create(Tenant tenant,long fence) {
        Properties p=new Properties();
        p.setProperty("name",tenant.connectorName()); p.setProperty("topic.prefix",tenant.connectorName());
        p.setProperty("connector.class","io.debezium.connector.sqlserver.SqlServerConnector");
        p.setProperty("database.hostname",tenant.host()); p.setProperty("database.port",Integer.toString(tenant.port()));
        p.setProperty("database.names",tenant.database()); p.setProperty("database.user",tenant.user()); p.setProperty("database.password",tenant.password());
        p.setProperty("database.encrypt",Boolean.toString(tenant.encrypt()));
        p.setProperty("database.trustServerCertificate",Boolean.toString(tenant.trustServerCertificate()));
        p.setProperty("database.applicationName","IQHR-CDC"); p.setProperty("database.loginTimeout","10"); p.setProperty("database.socketTimeout","30000");
        p.setProperty("table.include.list",String.join(",",tenant.capturedTables().stream().map(Pattern::quote).toList()));
        p.setProperty("snapshot.mode","no_data"); p.setProperty("snapshot.locking.mode","none");
        p.setProperty("schema.history.internal.store.only.captured.tables.ddl","true");
        p.setProperty("include.schema.changes","false"); p.setProperty("tombstones.on.delete","false");
        p.setProperty("event.processing.failure.handling.mode","fail"); p.setProperty("inconsistent.schema.handling.mode","fail");
        p.setProperty("decimal.handling.mode","string"); p.setProperty("binary.handling.mode","base64");
        p.setProperty("key.converter.schemas.enable","false"); p.setProperty("value.converter.schemas.enable","false");
        p.setProperty("heartbeat.interval.ms","10000"); p.setProperty("poll.interval.ms","1000");
        p.setProperty("max.queue.size",Integer.toString(tenant.maxQueueSize())); p.setProperty("max.batch.size",Integer.toString(tenant.maxBatchSize()));
        p.setProperty("max.queue.size.in.bytes","67108864"); p.setProperty("offset.flush.interval.ms","1000");
        p.setProperty("offset.flush.timeout.ms","30000"); p.setProperty("errors.max.retries","0");
        // Synchronous engine preserves a single ordered, confirmed batch boundary.
        p.setProperty("record.processing.threads","1"); p.setProperty("record.processing.order","ORDERED");
        p.setProperty("task.management.timeout.ms","30000");
        p.setProperty("offset.storage","io.debezium.storage.jdbc.offset.JdbcOffsetBackingStore");
        p.setProperty("schema.history.internal","io.debezium.storage.jdbc.history.JdbcSchemaHistory");
        for(String prefix:new String[]{"offset.storage.","schema.history.internal."}) {
            p.setProperty(prefix+"jdbc.connection.url",tenant.jdbcUrl()); p.setProperty(prefix+"jdbc.connection.user",tenant.user());
            p.setProperty(prefix+"jdbc.connection.password",tenant.password());
            p.setProperty(prefix+"jdbc.connection.retry.max.attempts","3");
        }
        p.setProperty("offset.storage.jdbc.table.name","IQHR_CdcOffset");
        p.setProperty("schema.history.internal.jdbc.table.name","IQHR_CdcSchemaHistory");
        // Flyway owns DDL. Library fallback must fail instead of constructing/resetting state.
        p.setProperty("offset.storage.jdbc.table.ddl","THROW 51000, 'Missing migrated CDC offset table', 1;");
        p.setProperty("schema.history.internal.jdbc.table.ddl","THROW 51000, 'Missing migrated CDC schema table', 1;");
        String guard="IF NOT EXISTS (SELECT 1 FROM dbo.IQHR_CdcActivation WITH (UPDLOCK,HOLDLOCK) WHERE source_id='"
                +tenant.sourceId()+"' AND fence="+fence+") THROW 51000, 'CDC source fenced', 1; ";
        p.setProperty("offset.storage.jdbc.table.select","SELECT id,offset_key,offset_val FROM dbo.%s ORDER BY record_insert_ts,record_insert_seq");
        p.setProperty("offset.storage.jdbc.table.delete",guard+"DELETE FROM dbo.%s");
        p.setProperty("offset.storage.jdbc.table.insert","INSERT INTO dbo.%s(id,offset_key,offset_val,record_insert_ts,record_insert_seq) VALUES(?,?,?,?,?)");
        p.setProperty("schema.history.internal.jdbc.table.select","SELECT id,history_data FROM dbo.%s ORDER BY storage_sequence");
        p.setProperty("schema.history.internal.jdbc.table.exists","SELECT TOP(1) id FROM dbo.%s");
        p.setProperty("schema.history.internal.jdbc.table.insert",guard+"INSERT INTO dbo.%s(id,history_data,history_data_seq,record_insert_ts,record_insert_seq) VALUES(?,?,?,?,?)");
        return p;
    }
}
