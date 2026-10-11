package com.iqhr.cdc.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iqhr.cdc.source.SqlServerControl.Capture;
import com.iqhr.cdc.source.SqlServerControl.Column;
import com.iqhr.cdc.source.SqlServerControl.TableObservation;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Online schema evolution for captured tables. A structural change never stops capture: the service adds a capture
 * instance with the current table shape, Debezium switches to it at its start LSN (refreshing its own schema), and the
 * superseded instance is retired once the durable position is past that switch. No gap is possible because both
 * instances capture concurrently until the retirement.
 */
public final class SchemaEvolution {
    public static final String CURRENT="CURRENT", SOURCE_CHANGED="SOURCE_CHANGED", READER_STALE="READER_STALE",
            REFRESHING="REFRESHING", REFRESH_FAILED="REFRESH_FAILED";
    /** Lets the reader pass the switch point in a later iteration before the old instance disappears. */
    static final Duration RETIRE_GRACE=Duration.ofMinutes(2);
    private static final DateTimeFormatter SUFFIX=DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final int MAX_CHANGES=12;

    private SchemaEvolution() {}

    /** One column difference, e.g. ADDED Note nvarchar(200) or CHANGED Floor varchar(10) → varchar(20). */
    public record Change(String column,String kind,String before,String after) {}
    public record Assessment(String state,List<Change> source,List<Change> reader) {
        public boolean needsRefresh() {return SOURCE_CHANGED.equals(state)||READER_STALE.equals(state);}
    }
    public sealed interface Action permits Create, Retire {String schema();String table();}
    public record Create(String schema,String table,Capture template,String captureInstance) implements Action {}
    public record Retire(String schema,String table,String captureInstance) implements Action {}

    /**
     * Compares the newest capture instance with the source table (columns SQL Server will not add to an existing
     * instance) and with the reader's schema history (in-place type changes Debezium has not seen).
     */
    public static Assessment assess(TableObservation observed,List<Column> readerColumns) {
        if(observed.captures().isEmpty())return new Assessment(CURRENT,List.of(),List.of());
        if(observed.captures().size()>1)return new Assessment(REFRESHING,List.of(),List.of());
        List<Column> captured=observed.captures().getLast().columns();
        List<Change> source=observed.sourceColumns().isEmpty()?List.of():diff(captured,observed.sourceColumns());
        List<Change> reader=readerColumns==null?List.of():diff(readerColumns,captured);
        String state=!reader.isEmpty()?READER_STALE:!source.isEmpty()?SOURCE_CHANGED:CURRENT;
        return new Assessment(state,source,reader);
    }

    /**
     * Next step for an active table, or empty. A refresh is created on an Admin request, or automatically for a
     * difference that no earlier attempt addressed (a failed or ineffective refresh is not retried in a loop).
     * Retirement waits until the durable position is past the new instance's start.
     */
    public static Optional<Action> plan(TableObservation observed,Assessment assessment,String savedLsn,boolean requested,
                                        boolean automatic,String detail,String attemptedDetail,Instant lastAttempt,Instant now) {
        List<Capture> captures=observed.captures();
        if(captures.size()>1) {
            Capture newest=captures.getLast();
            boolean passed=savedLsn!=null&&newest.ready()&&SqlServerControl.compare(SqlServerControl.parseLsn(savedLsn),newest.start())>0;
            boolean settled=lastAttempt==null||!lastAttempt.plus(RETIRE_GRACE).isAfter(now);
            return passed&&settled?Optional.of(new Retire(observed.schema(),observed.table(),captures.getFirst().name())):Optional.empty();
        }
        if(captures.size()!=1||!captures.getFirst().ready())return Optional.empty();
        boolean due=requested||(automatic&&assessment.needsRefresh()&&!Objects.equals(detail,attemptedDetail));
        if(!due)return Optional.empty();
        return Optional.of(new Create(observed.schema(),observed.table(),captures.getFirst(),
                instanceName(observed.schema(),observed.table(),captures.getFirst().name(),now)));
    }

    /** SQL Server capture instance names are sysname values limited to 100 characters. */
    static String instanceName(String schema,String table,String current,Instant now) {
        String base=(schema+"_"+table);
        if(base.length()>84)base=base.substring(0,84);
        String name=base+"_"+SUFFIX.format(LocalDateTime.ofInstant(now,java.time.ZoneOffset.UTC));
        return name.equalsIgnoreCase(current)?name+"b":name;
    }

    /** Latest reader (Debezium schema history) column list per qualified table name, ignoring other databases. */
    public static Map<String,List<Column>> readerSchemas(List<String> history,ObjectMapper mapper,String database) {
        Map<String,List<Column>> tables=new HashMap<>();
        for(String record:history) {
            JsonNode root;
            try {root=mapper.readTree(record);} catch(Exception e) {continue;}
            for(JsonNode change:root.path("tableChanges")) {
                String[] id=parseId(change.path("id").asText(""));
                if(id==null||!id[0].equalsIgnoreCase(database))continue;
                String name=id[1]+"."+id[2];
                if("DROP".equals(change.path("type").asText())) {tables.remove(name);continue;}
                List<Column> columns=new ArrayList<>();
                for(JsonNode column:change.path("table").path("columns")) {
                    String type=column.path("typeName").asText("").toLowerCase(Locale.ROOT).replace(" identity","").trim();
                    columns.add(new Column(column.path("name").asText(),type,column.path("length").asInt(0),
                            column.path("length").asInt(0),column.path("scale").asInt(0),column.path("optional").asBoolean(true)));
                }
                tables.put(name,List.copyOf(columns));
            }
        }
        return tables;
    }

    /** Debezium ids look like "db"."schema"."table". */
    private static String[] parseId(String id) {
        String[] parts=id.split("\\.");
        if(parts.length!=3)return null;
        for(int i=0;i<3;i++)parts[i]=parts[i].replaceAll("^\"|\"$","");
        return parts;
    }

    /** Differences from {@code before} to {@code after} that change how values are captured or decoded. */
    static List<Change> diff(List<Column> before,List<Column> after) {
        Map<String,Column> old=new LinkedHashMap<>();
        before.forEach(column -> old.put(column.name().toLowerCase(Locale.ROOT),column));
        List<Change> changes=new ArrayList<>();
        for(Column column:after) {
            Column previous=old.remove(column.name().toLowerCase(Locale.ROOT));
            if(previous==null)changes.add(new Change(column.name(),"ADDED",null,describe(column)));
            else if(!signature(previous).equals(signature(column)))changes.add(new Change(column.name(),"CHANGED",describe(previous),describe(column)));
        }
        old.values().forEach(column -> changes.add(new Change(column.name(),"REMOVED",describe(column),null)));
        return changes.size()>MAX_CHANGES?List.copyOf(changes.subList(0,MAX_CHANGES)):List.copyOf(changes);
    }

    /** Type identity that matters for capture/decoding; lengths are compared only where they constrain values. */
    static String signature(Column column) {
        String type=column.type();
        return switch(type) {
            case "char","varchar","nchar","nvarchar","binary","varbinary" -> type+"("+length(column.length())+")";
            case "decimal","numeric" -> type+"("+column.precision()+","+column.scale()+")";
            default -> type;
        };
    }
    static String describe(Column column) {return signature(column);}
    /** -1 (SQL Server) and Debezium's large sentinel lengths both mean max. */
    private static String length(int value) {return value<=0||value>8000?"max":Integer.toString(value);}

    /** Compact JSON for the registry/Admin, bounded to the column size. */
    public static String detail(Assessment assessment,ObjectMapper mapper) {
        if(assessment.source().isEmpty()&&assessment.reader().isEmpty())return null;
        try {
            String json=mapper.writeValueAsString(Map.of("source",assessment.source(),"reader",assessment.reader()));
            return json.length()<=2000?json:mapper.writeValueAsString(Map.of("source",assessment.source().subList(0,Math.min(4,assessment.source().size())),
                    "reader",assessment.reader().subList(0,Math.min(4,assessment.reader().size())),"truncated",true));
        } catch(Exception e) {return null;}
    }
}
