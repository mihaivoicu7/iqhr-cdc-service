package com.iqhr.cdc.maintenance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iqhr.cdc.CdcProperties.Tenant;
import com.iqhr.cdc.model.*;
import com.iqhr.cdc.source.RecoveryGuard;
import com.iqhr.cdc.source.SqlServerControl;
import com.iqhr.cdc.store.TenantStore;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import java.nio.file.*;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import org.hibernate.Session;

/** Explicit one-off TEST cleanup. This class is not packaged in the service or invoked at startup. */
public final class CleanupTestProbes {
    static final ObjectMapper MAPPER=new ObjectMapper().findAndRegisterModules();
    static final List<String> OLD_TABLES=List.of("dbo.HR_EmployeeContractInfo","dbo.CodexCdcProbe","dbo.CodexCdcProbeOther");
    static final List<String> NEXT_TABLES=List.of("dbo.HR_Employee","dbo.HR_EmployeeContractInfo");
    static final List<String> PROBES=List.of("CodexCdcProbe","CodexCdcProbeOther");
    static final String SOURCE="employee-contract-info";

    public static void main(String[] args) {
        try {
            require(args.length==1&&"--apply".equals(args[0]),"EXPLICIT_APPLY_REQUIRED");
            JsonNode configuration=MAPPER.readTree(Files.readString(path("CDC_TEST_CONFIG")));
            require("TECHNO".equals(configuration.path("tenantId").asText())
                    &&"192.168.99.133".equals(configuration.path("server").asText())
                    &&"tdev_technophar".equals(configuration.path("database").asText()),"WRONG_TEST_DATABASE");
            require(configuration.path("username").isTextual()&&configuration.path("password").isTextual(),"TEST_CREDENTIALS_REQUIRED");
            Tenant oldTenant=tenant(configuration,OLD_TABLES),nextTenant=tenant(configuration,NEXT_TABLES);
            Expected expected=Expected.read(MAPPER.readTree(Files.readString(path("CDC_CUTOVER_BACKUP"))));
            verifyReceipt(MAPPER.readTree(Files.readString(path("CDC_DRAIN_RECEIPT"))),Instant.now());
            // Same database-wide session lock used by the live reader. No engine or JMS producer starts.
            try(SqlServerControl control=new SqlServerControl(nextTenant);TenantStore store=TenantStore.open(nextTenant)) {
                control.assertOwnership();
                SourceActivation activation=store.activation();
                String savedLsn=RecoveryGuard.savedLsn(activation,store.offsets(),oldTenant,MAPPER);
                control.inspect(savedLsn); // Newly included HR_Employee must cover the intact saved position.
                verifyReceipt(MAPPER.readTree(Files.readString(path("CDC_DRAIN_RECEIPT"))),Instant.now());
                int removed=store.transaction(em -> {
                    control.assertOwnership();
                    em.unwrap(Session.class).doWork(connection -> verifyNewTableBoundary(connection,savedLsn));
                    int count=applyEntityChanges(em,expected,oldTenant,nextTenant);
                    // SQL is restricted to SQL Server CDC metadata, source-table emptiness guards and final DDL.
                    // These exact synthetic objects are permanently removed under the user's explicit request.
                    em.flush();
                    em.unwrap(Session.class).doWork(CleanupTestProbes::removeEmptyProbeObjects);
                    verifyPreservedState(em,expected);
                    control.assertOwnership();
                    return count;
                });
                System.out.println(MAPPER.writeValueAsString(Map.of("committed",true,"removedSyntheticEvents",removed,
                        "sourceId",SOURCE,"onboardingCommitLsn",savedLsn,"newHash",TenantStore.configurationHash(nextTenant),
                        "newFence",expected.fence()+1,"offsetAndSchemaPreserved",true)));
            }
        } catch(Exception error) {
            System.err.println(safeCode(error));
            System.exit(1);
        }
    }

    static Tenant tenant(JsonNode config,List<String> tables) {
        return new Tenant("TECHNO",SOURCE,"192.168.99.133",1433,"tdev_technophar",config.path("username").asText(),
                config.path("password").asText(),false,true,tables,true,20160,1024,128);
    }
    static Path path(String name) {
        String value=System.getenv(name);require(value!=null&&!value.isBlank(),"MISSING_"+name);return Path.of(value);
    }
    static void verifyReceipt(JsonNode receipt,Instant now) {
        require("iqhr.cdc.TECHNO.history".equals(receipt.path("queue").asText())
                &&"exited".equals(receipt.path("serviceState").asText()),"INVALID_STOPPED_SERVICE_RECEIPT");
        for(String field:List.of("MessageCount","DeliveringCount","ConsumerCount"))
            require(receipt.path(field).isIntegralNumber()&&receipt.path(field).longValue()==0,"BROKER_NOT_DRAINED");
        Instant checked;
        try {checked=Instant.parse(receipt.path("checkedAt").asText());}
        catch(RuntimeException e) {throw new GuardFailure("INVALID_RECEIPT_TIME");}
        long age=java.time.Duration.between(checked,now).toMillis();
        require(age>=0&&age<120000,"STALE_DRAIN_RECEIPT");
    }

    static int applyEntityChanges(EntityManager em,Expected expected,Tenant oldTenant,Tenant nextTenant) {
        var allActivations=em.createQuery("from SourceActivation",SourceActivation.class).setMaxResults(2).getResultList();
        require(allActivations.size()==1,"AMBIGUOUS_ACTIVATION");
        SourceActivation activation=em.find(SourceActivation.class,SOURCE,LockModeType.PESSIMISTIC_WRITE);
        require(activation!=null&&"TECHNO".equals(activation.tenantId)&&SOURCE.equals(activation.sourceId)
                &&activation.fence==expected.fence()&&activation.incarnation.equals(expected.incarnation())
                &&activation.configurationHash.equals(expected.oldHash())
                &&activation.configurationHash.equals(TenantStore.configurationHash(oldTenant)),"ACTIVATION_CHANGED");
        verifyPreservedState(em,expected);
        var fixtures=em.createQuery("from HistoryEvent e where e.tenantId=:tenant and e.sourceId=:source and e.schemaName=:schema and e.tableName in :tables",HistoryEvent.class)
                .setParameter("tenant","TECHNO").setParameter("source",SOURCE).setParameter("schema","dbo").setParameter("tables",PROBES).getResultList();
        require(new TreeSet<>(fixtures.stream().map(event -> event.eventId).toList()).equals(expected.eventIds()),"SYNTHETIC_HISTORY_CHANGED");
        fixtures.forEach(em::remove);
        activation.configurationHash=TenantStore.configurationHash(nextTenant);
        activation.fence=Math.addExact(activation.fence,1);
        return fixtures.size();
    }

    static void verifyPreservedState(EntityManager em,Expected expected) {
        List<OffsetRow> offsets=em.createQuery("from OffsetRow",OffsetRow.class).setMaxResults(2).getResultList();
        require(offsets.size()==1,"CHECKPOINT_PARTITIONS_CHANGED");
        OffsetRow row=offsets.getFirst();
        require(expected.offsetId().equals(row.id)&&expected.offsetKey().equals(row.key)&&expected.offsetValue().equals(row.value),"CHECKPOINT_CHANGED");
        List<Long> schemaSequences=em.createQuery("select e.sequence from SchemaHistoryRow e order by e.sequence",Long.class).getResultList();
        require(schemaSequences.equals(expected.schemaSequences()),"SCHEMA_HISTORY_CHANGED");
        SourceActivation activation=em.find(SourceActivation.class,SOURCE);
        require(activation!=null&&activation.incarnation.equals(expected.incarnation()),"INCARNATION_CHANGED");
    }

    static void removeEmptyProbeObjects(Connection connection) throws SQLException {
        require(!connection.getAutoCommit(),"DDL_MUST_SHARE_JPA_TRANSACTION");
        String sql="""
            IF OBJECT_ID(N'dbo.CodexCdcProbe',N'U') IS NULL OR OBJECT_ID(N'dbo.CodexCdcProbeOther',N'U') IS NULL
                THROW 51000,'Expected owned probe tables are missing; review cleanup state',1;
            IF EXISTS(SELECT 1 FROM dbo.CodexCdcProbe WITH(TABLOCKX,HOLDLOCK))
                OR EXISTS(SELECT 1 FROM dbo.CodexCdcProbeOther WITH(TABLOCKX,HOLDLOCK))
                THROW 51000,'Probe source rows are not empty; review before cleanup',1;
            IF (SELECT COUNT(*) FROM cdc.change_tables WHERE source_object_id=OBJECT_ID(N'dbo.CodexCdcProbe'))<>1
                OR NOT EXISTS(SELECT 1 FROM cdc.change_tables WHERE source_object_id=OBJECT_ID(N'dbo.CodexCdcProbe') AND capture_instance=N'dbo_CodexCdcProbe')
                OR (SELECT COUNT(*) FROM cdc.change_tables WHERE source_object_id=OBJECT_ID(N'dbo.CodexCdcProbeOther'))<>1
                OR NOT EXISTS(SELECT 1 FROM cdc.change_tables WHERE source_object_id=OBJECT_ID(N'dbo.CodexCdcProbeOther') AND capture_instance=N'dbo_CodexCdcProbeOther')
                THROW 51000,'Unexpected probe CDC capture instances; review before cleanup',1;
            EXEC sys.sp_cdc_disable_table @source_schema=N'dbo',@source_name=N'CodexCdcProbe',@capture_instance=N'dbo_CodexCdcProbe';
            EXEC sys.sp_cdc_disable_table @source_schema=N'dbo',@source_name=N'CodexCdcProbeOther',@capture_instance=N'dbo_CodexCdcProbeOther';
            DROP TABLE dbo.CodexCdcProbe;
            DROP TABLE dbo.CodexCdcProbeOther;
            """;
        try(Statement statement=connection.createStatement()) {
            statement.setQueryTimeout(30);
            boolean result=statement.execute(sql);
            while(result||statement.getUpdateCount()!=-1) {
                result=statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
            }
        }
    }

    static void verifyNewTableBoundary(Connection connection,String savedLsn) throws SQLException {
        try(PreparedStatement statement=connection.prepareStatement("SELECT TOP(1) 1 FROM cdc.dbo_HR_Employee_CT WHERE __$start_lsn<=?")) {
            statement.setQueryTimeout(15);statement.setBytes(1,SqlServerControl.parseLsn(savedLsn));
            try(ResultSet rows=statement.executeQuery()) {
                require(!rows.next(),"EMPLOYEE_CHANGES_EXIST_BEFORE_ONBOARDING_BOUNDARY");
            }
        }
    }

    static void require(boolean condition,String code) {if(!condition)throw new GuardFailure(code);}
    static long positiveLong(JsonNode value,String code) {
        try {
            require(value!=null&&(value.isIntegralNumber()||value.isTextual())&&value.asText().matches("[0-9]+"),code);
            long parsed=Long.parseLong(value.asText());require(parsed>0,code);return parsed;
        } catch(NumberFormatException error) {throw new GuardFailure(code);}
    }
    static final class GuardFailure extends RuntimeException {GuardFailure(String code){super(code);}}
    static String safeCode(Throwable error) {
        StringBuilder result=new StringBuilder("CLEANUP_FAILED");
        for(int depth=0;error!=null&&depth<6;error=error.getCause(),depth++) {
            if(error instanceof GuardFailure)return "CLEANUP_REJECTED_"+error.getMessage();
            result.append('_').append(error.getClass().getSimpleName());
            if(error instanceof SQLException sql)result.append("_SQLCODE_").append(sql.getErrorCode());
        }
        return result.toString();
    }
    record Expected(String oldHash,long fence,String incarnation,String offsetId,String offsetKey,String offsetValue,
                    List<Long> schemaSequences,Set<String> eventIds) {
        static Expected read(JsonNode backup) {
            require(backup.isArray()&&backup.size()>=4&&backup.get(0).size()==1&&backup.get(1).size()==1,"INVALID_BACKUP");
            JsonNode activation=backup.get(0).get(0),offset=backup.get(1).get(0);
            require("TECHNO".equals(activation.path("tenant_id").asText())&&SOURCE.equals(activation.path("source_id").asText()),"BACKUP_TENANT_MISMATCH");
            List<Long> sequences=new ArrayList<>();backup.get(2).forEach(row -> sequences.add(positiveLong(row.get("storage_sequence"),"INVALID_BACKUP_SCHEMA_SEQUENCE")));
            Set<String> ids=new TreeSet<>();
            backup.get(3).forEach(row -> {
                require("TECHNO".equals(row.path("tenant_id").asText())&&SOURCE.equals(row.path("source_id").asText())
                        &&"dbo".equals(row.path("schema_name").asText())&&PROBES.contains(row.path("table_name").asText()),"BACKUP_HISTORY_SCOPE_MISMATCH");
                require(ids.add(row.path("event_id").asText()),"BACKUP_DUPLICATE_EVENT");
            });
            return new Expected(activation.path("configuration_hash").asText(),positiveLong(activation.get("fence"),"INVALID_BACKUP_FENCE"),
                    activation.path("incarnation").asText(),offset.path("id").asText(),offset.path("offset_key").asText(),
                    offset.path("offset_val").asText(),List.copyOf(sequences),Set.copyOf(ids));
        }
    }
}
