package com.iqhr.cdc.source;

import com.fasterxml.jackson.databind.*;
import com.iqhr.cdc.CdcProperties.Tenant;
import com.iqhr.cdc.model.*;
import com.iqhr.cdc.store.ContinuityException;
import com.iqhr.cdc.store.TenantStore;
import java.util.List;

public final class RecoveryGuard {
    private RecoveryGuard() {}
    public static String savedLsn(SourceActivation activation,List<OffsetRow> offsets,Tenant tenant,ObjectMapper mapper) {
        if(activation==null) {
            if(!offsets.isEmpty())throw new ContinuityException("ACTIVATION_MISSING_FOR_EXISTING_OFFSETS");
            return null;
        }
        if(!tenant.id().equals(activation.tenantId)||!TenantStore.configurationHash(tenant).equals(activation.configurationHash))
            throw new ContinuityException("SOURCE_CONFIGURATION_CHANGED");
        if(offsets.size()!=1)throw new ContinuityException("SOURCE_OFFSETS_MISSING_OR_AMBIGUOUS");
        try {
            OffsetRow row=offsets.getFirst();
            JsonNode key=mapper.readTree(row.key),offset=mapper.readTree(row.value);
            if(!offset.isObject())throw new ContinuityException("SOURCE_OFFSETS_CORRUPT");
            if(!key.isArray() || key.size()!=2 || !tenant.connectorName().equals(key.get(0).asText())
                    || !tenant.connectorName().equals(key.get(1).path("server").asText())
                    || !tenant.database().equals(key.get(1).path("database").asText()))
                throw new ContinuityException("SOURCE_OFFSET_PARTITION_CHANGED");
            if(offset.hasNonNull("snapshot") && !"false".equals(offset.path("snapshot").asText()) && !offset.path("snapshot_completed").asBoolean(false))
                throw new ContinuityException("SOURCE_INITIALIZATION_INCOMPLETE");
            if(!offset.has("change_lsn")||!offset.path("event_serial_no").isIntegralNumber()
                    ||!offset.path("event_serial_no").canConvertToLong()||offset.path("event_serial_no").longValue()<0)
                throw new ContinuityException("SOURCE_REPLAY_COORDINATES_INVALID");
            boolean completedSnapshot=offset.hasNonNull("snapshot")&&offset.path("snapshot_completed").asBoolean(false);
            JsonNode change=offset.get("change_lsn");
            if(change.isNull()||"NULL".equals(change.asText())) {
                // SQL Server no_data initialization keeps a concrete maximum commit LSN but has
                // no in-transaction change position. Debezium clears snapshot markers afterwards
                // and serializes its initial Lsn.NULL with constructor-default event serial 1.
                boolean initialBoundary=change.isTextual()&&"NULL".equals(change.textValue())
                        &&offset.path("event_serial_no").longValue()==1;
                if(!completedSnapshot&&!initialBoundary)throw new ContinuityException("SOURCE_REPLAY_COORDINATES_INVALID");
            } else { SqlServerControl.parseLsn(change.asText(null)); }
            String lsn=offset.path("commit_lsn").asText(null);
            if(completedSnapshot&&(lsn==null||"NULL".equals(lsn)))throw new ContinuityException("SOURCE_INITIALIZATION_INCOMPLETE");
            SqlServerControl.parseLsn(lsn); return lsn;
        } catch(ContinuityException e) { throw e; }
        catch(Exception e) { throw new ContinuityException("SOURCE_OFFSETS_CORRUPT"); }
    }
}
