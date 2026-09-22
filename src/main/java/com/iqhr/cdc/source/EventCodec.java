package com.iqhr.cdc.source;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.iqhr.cdc.CdcProperties.Tenant;
import com.iqhr.cdc.CdcProperties;
import com.iqhr.cdc.model.HistoryEvent;
import com.iqhr.cdc.store.ContinuityException;
import com.iqhr.cdc.store.TenantStore;
import java.time.Instant;
import java.util.*;

public final class EventCodec {
    private final ObjectMapper mapper;
    private final Tenant tenant;
    private final String incarnation;
    private final Map<String,String> activationLsns;
    public EventCodec(ObjectMapper mapper,Tenant tenant,String incarnation) {
        this(mapper,tenant,incarnation,Map.of());
    }
    public EventCodec(ObjectMapper mapper,Tenant tenant,String incarnation,Map<String,String> activationLsns) {
        this.mapper=mapper; this.tenant=tenant; this.incarnation=incarnation;
        this.activationLsns=Map.copyOf(activationLsns);
    }
    public HistoryEvent parse(String keyJson,String valueJson,String destination) {
        if(valueJson==null)return null; // Kafka tombstone after an already-emitted DELETE.
        JsonNode value=read(valueJson);
        if(destination.equals("__debezium-heartbeat."+tenant.connectorName()))return null;
        value=payload(value);
        String op=text(value,"op");
        String operation=switch(op) { case "c" -> "INSERT"; case "u" -> "UPDATE"; case "d" -> "DELETE";
            default -> throw new ContinuityException("UNEXPECTED_SOURCE_OPERATION"); };
        JsonNode source=value.path("source");
        String schema=text(source,"schema"), table=text(source,"table");
        if(!tenant.database().equals(text(source,"db"))||!tenant.capturedTables().contains(schema+"."+table))
            throw new ContinuityException("UNEXPECTED_SOURCE_TABLE");
        JsonNode key=keyJson==null?NullNode.instance:payload(read(keyJson));
        if(!key.isObject()||key.isEmpty())throw new ContinuityException("SOURCE_KEY_MISSING");
        HistoryEvent event=new HistoryEvent();
        event.tenantId=tenant.id(); event.sourceId=tenant.sourceId(); event.schemaName=schema; event.tableName=table;
        event.operation=operation; event.commitLsn=text(source,"commit_lsn"); event.changeLsn=text(source,"change_lsn");
        SqlServerControl.parseLsn(event.commitLsn); SqlServerControl.parseLsn(event.changeLsn);
        if(!source.path("event_serial_no").canConvertToLong()||!source.path("ts_ms").canConvertToLong())
            throw new ContinuityException("SOURCE_COORDINATES_MISSING");
        if(CdcProperties.HEARTBEAT_TABLE.equals(schema+"."+table)) {
            if(!key.path("singleton_id").isIntegralNumber()||key.path("singleton_id").intValue()!=1
                    || !("INSERT".equals(operation)||"UPDATE".equals(operation)))throw new ContinuityException("INVALID_INTERNAL_HEARTBEAT");
            return null; // Ordered engine acknowledgement advances a real committed source position only.
        }
        String activation=activationLsns.get(schema+"."+table);
        if(activation!=null&&Arrays.compareUnsigned(SqlServerControl.parseLsn(event.commitLsn),SqlServerControl.parseLsn(activation))<=0)
            return null; // User enablement starts strictly after the durable cutover commit, without backfill.
        event.eventSerialNo=source.path("event_serial_no").longValue();
        event.occurredAt=Instant.ofEpochMilli(source.path("ts_ms").longValue()); event.capturedAt=Instant.now();
        JsonNode before=value.get("before"),after=value.get("after");
        if((!"INSERT".equals(operation)&&!isObject(before))||(!"DELETE".equals(operation)&&!isObject(after)))
            throw new ContinuityException("SOURCE_ROW_IMAGE_MISSING");
        event.recordKey=write(canonical(key)); event.beforeJson=isObject(before)?write(before):null; event.afterJson=isObject(after)?write(after):null;
        SortedSet<String> fields=new TreeSet<>();
        if(isObject(before))before.fieldNames().forEachRemaining(fields::add);
        if(isObject(after))after.fieldNames().forEachRemaining(fields::add);
        JsonNode b=before,a=after;
        fields.removeIf(field -> Objects.equals(isObject(b)?b.get(field):null,isObject(a)?a.get(field):null));
        event.changedProperties=write(fields); event.eventId=identity(event);
        return event;
    }
    public String identity(HistoryEvent event) {
        return TenantStore.sha256(write(List.of(tenant.id(),tenant.sourceId(),incarnation,event.schemaName,event.tableName,
                event.commitLsn,event.changeLsn,event.eventSerialNo,event.operation,read(event.recordKey))));
    }
    public String encode(HistoryEvent event) { return write(new WireEvent(1,incarnation,event)); }
    public HistoryEvent decode(String json) {
        try {
            WireEvent envelope=mapper.readValue(json,WireEvent.class);
            HistoryEvent event=envelope.event();
            if(envelope.version()!=1||!incarnation.equals(envelope.incarnation())||event==null
                    ||!tenant.id().equals(event.tenantId)||!tenant.sourceId().equals(event.sourceId)
                    ||!tenant.tables().contains(event.schemaName+"."+event.tableName)||!identity(event).equals(event.eventId))
                throw new ContinuityException("INVALID_BROKER_EVENT");
            return event;
        } catch(ContinuityException e) { throw e; }
        catch(Exception e) { throw new ContinuityException("INVALID_BROKER_EVENT"); }
    }
    private static boolean isObject(JsonNode value) { return value!=null&&value.isObject(); }
    private static JsonNode payload(JsonNode node) { return node.has("schema")&&node.has("payload")?node.get("payload"):node; }
    private static String text(JsonNode node,String field) {
        if(!node.path(field).isTextual()||node.path(field).textValue().isEmpty())throw new ContinuityException("INVALID_SOURCE_ENVELOPE");
        return node.path(field).textValue();
    }
    private JsonNode canonical(JsonNode node) {
        if(node.isObject()) { ObjectNode sorted=mapper.createObjectNode(); TreeSet<String> fields=new TreeSet<>(); node.fieldNames().forEachRemaining(fields::add);
            fields.forEach(field -> sorted.set(field,canonical(node.get(field)))); return sorted; }
        if(node.isArray()) { ArrayNode result=mapper.createArrayNode(); node.forEach(child -> result.add(canonical(child))); return result; }
        return node;
    }
    private JsonNode read(String value) {
        try { return mapper.readTree(value); } catch(Exception e) { throw new ContinuityException("INVALID_EVENT_JSON"); }
    }
    private String write(Object value) {
        try { return mapper.writeValueAsString(value); } catch(Exception e) { throw new ContinuityException("EVENT_SERIALIZATION_FAILED"); }
    }
    public record WireEvent(int version,String incarnation,HistoryEvent event) {}
}
