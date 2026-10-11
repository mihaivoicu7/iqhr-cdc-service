package com.iqhr.cdc;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.iqhr.cdc.model.CdcTable;
import com.iqhr.cdc.source.SchemaEvolution;
import com.iqhr.cdc.source.SqlServerControl;
import com.iqhr.cdc.source.SqlServerControl.Capture;
import com.iqhr.cdc.source.SqlServerControl.Column;
import com.iqhr.cdc.source.SqlServerControl.TableObservation;
import com.iqhr.cdc.store.TableRegistry;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import org.junit.jupiter.api.Test;

class SchemaEvolutionTest {
    static final String SAVED="00000027:00000ac0:0003";
    static final Instant NOW=Instant.parse("2026-10-11T12:00:00Z");
    static Column text(String name,int length) {return new Column(name,"varchar",length,0,0,true);}
    static Column number(String name) {return new Column(name,"int",4,10,0,false);}
    static Capture capture(String name,String start,List<Column> columns) {
        return new Capture(name,LocalDateTime.parse("2026-09-22T14:15:38").plusSeconds(name.length()),SqlServerControl.parseLsn(start),true,
                null,null,true,"PK_HR_EmployeeAddress",columns);
    }
    static TableObservation table(List<Column> source,Capture... captures) {
        return new TableObservation("dbo","HR_EmployeeAddress","fingerprint",true,null,
                Arrays.stream(captures).map(Capture::start).toList(),List.of(captures),source);
    }
    static final List<Column> WIDENED=List.of(number("ID"),text("Floor",20),text("Apartment",20));
    static final List<Column> READER=List.of(number("ID"),text("Floor",10),text("Apartment",5));

    @Test void inPlaceWideningIsDetectedAgainstTheReaderAndRefreshedOnce() {
        var observed=table(WIDENED,capture("dbo_HR_EmployeeAddress","00000027:00000a00:0001",WIDENED));
        var assessment=SchemaEvolution.assess(observed,READER);
        assertThat(assessment.state()).isEqualTo(SchemaEvolution.READER_STALE);
        assertThat(assessment.reader()).containsExactly(
                new SchemaEvolution.Change("Floor","CHANGED","varchar(10)","varchar(20)"),
                new SchemaEvolution.Change("Apartment","CHANGED","varchar(5)","varchar(20)"));
        String detail=SchemaEvolution.detail(assessment,Fixtures.MAPPER);
        var create=SchemaEvolution.plan(observed,assessment,SAVED,false,true,detail,null,null,NOW).orElseThrow();
        assertThat(create).isInstanceOf(SchemaEvolution.Create.class);
        assertThat(((SchemaEvolution.Create)create).captureInstance()).isEqualTo("dbo_HR_EmployeeAddress_20261011120000").hasSizeLessThanOrEqualTo(100);
        // The same difference after an attempt (failed or ineffective) is not retried automatically, only on request.
        assertThat(SchemaEvolution.plan(observed,assessment,SAVED,false,true,detail,detail,NOW,NOW.plusSeconds(3600))).isEmpty();
        assertThat(SchemaEvolution.plan(observed,assessment,SAVED,true,true,detail,detail,NOW,NOW)).isPresent();
        assertThat(SchemaEvolution.plan(observed,assessment,SAVED,false,false,detail,null,null,NOW)).isEmpty();
    }
    @Test void sourceColumnsMissingFromTheCaptureNeedARefreshAndUnchangedTablesDoNot() {
        var captured=List.of(number("ID"),text("Floor",20));
        var added=SchemaEvolution.assess(table(WIDENED,capture("dbo_HR_EmployeeAddress","00000027:00000a00:0001",captured)),captured);
        assertThat(added.state()).isEqualTo(SchemaEvolution.SOURCE_CHANGED);
        assertThat(added.source()).containsExactly(new SchemaEvolution.Change("Apartment","ADDED",null,"varchar(20)"));
        var current=SchemaEvolution.assess(table(WIDENED,capture("dbo_HR_EmployeeAddress","00000027:00000a00:0001",WIDENED)),WIDENED);
        assertThat(current.state()).isEqualTo(SchemaEvolution.CURRENT);assertThat(current.needsRefresh()).isFalse();
        // Debezium's identity suffix and max sentinels are not structural differences.
        var reader=List.of(new Column("ID","int",10,10,0,false),new Column("Note","nvarchar",2147483647,0,0,true));
        var source=List.of(number("ID"),new Column("Note","nvarchar",-1,0,0,true));
        assertThat(SchemaEvolution.assess(table(source,capture("x","00000027:00000a00:0001",source)),reader).state()).isEqualTo(SchemaEvolution.CURRENT);
    }
    @Test void supersededInstanceIsRetiredOnlyAfterTheReaderPassedTheReplacementStart() {
        var old=capture("dbo_HR_EmployeeAddress","00000027:00000a00:0001",WIDENED);
        var replacement=capture("dbo_HR_EmployeeAddress_20261011120000","00000027:00000ab0:0001",WIDENED);
        var observed=table(WIDENED,old,replacement);
        var assessment=SchemaEvolution.assess(observed,READER);
        assertThat(assessment.state()).isEqualTo(SchemaEvolution.REFRESHING);
        assertThat(SchemaEvolution.plan(observed,assessment,"00000027:00000aa0:0001",false,true,null,null,NOW,NOW.plusSeconds(600))).isEmpty();
        assertThat(SchemaEvolution.plan(observed,assessment,SAVED,false,true,null,null,NOW,NOW.plusSeconds(30))).isEmpty();
        assertThat(SchemaEvolution.plan(observed,assessment,SAVED,false,true,null,null,NOW,NOW.plusSeconds(600)))
                .contains(new SchemaEvolution.Retire("dbo","HR_EmployeeAddress","dbo_HR_EmployeeAddress"));
    }
    @Test void readerSchemaFollowsTheLatestRecordForThisDatabaseOnly() {
        String create="""
            {"tableChanges":[{"type":"CREATE","id":"\\"tdev_technophar\\".\\"dbo\\".\\"HR_EmployeeAddress\\"","table":{"columns":[
              {"name":"ID","typeName":"int identity","length":10},{"name":"Floor","typeName":"varchar","length":10}]}}]}""";
        String alter=create.replace("CREATE","ALTER").replace("\"length\":10}]","\"length\":20}]");
        String other=create.replace("tdev_technophar","another_db");
        var schemas=SchemaEvolution.readerSchemas(List.of(create,"not json",alter,other),Fixtures.MAPPER,"tdev_technophar");
        assertThat(schemas).containsOnlyKeys("dbo.HR_EmployeeAddress");
        assertThat(schemas.get("dbo.HR_EmployeeAddress")).extracting(Column::name,Column::type,Column::length)
                .containsExactly(tuple("ID","int",10),tuple("Floor","varchar",20));
    }
    @Test void registryStartsOneRefreshPerDifferenceAndReportsMissingPermission() throws Exception {
        try(var store=TableRegistryTest.store(TableRegistryTest.database(),"create-only")) {
            var owner=store.claim();var registry=new TableRegistry(store,Fixtures.TENANT);
            var observed=new TableObservation("dbo","HR_EmployeeContractInfo","fingerprint",true,null,
                    List.of(SqlServerControl.parseLsn("00000027:00000a00:0001")),
                    List.of(capture("dbo_HR_EmployeeContractInfo","00000027:00000a00:0001",WIDENED)),WIDENED);
            registry.initialize(List.of(observed));registry.observe(List.of(observed),SAVED,owner.fence);
            // Debezium's stored schema still has the narrow columns.
            store.transaction(em -> {var history=new com.iqhr.cdc.model.SchemaHistoryRow();history.sequence=1;history.recordId="r1";
                history.data="""
                    {"tableChanges":[{"type":"CREATE","id":"\\"tdev_technophar\\".\\"dbo\\".\\"HR_EmployeeContractInfo\\"","table":{"columns":[
                      {"name":"ID","typeName":"int","length":10},{"name":"Floor","typeName":"varchar","length":10},{"name":"Apartment","typeName":"varchar","length":5}]}}]}""";
                em.persist(history);return null;});
            var reader=SchemaEvolution.readerSchemas(store.schemaHistory(),Fixtures.MAPPER,"tdev_technophar");
            assertThat(reader.get("dbo.HR_EmployeeContractInfo")).hasSize(3);
            var actions=registry.planSchemaEvolution(List.of(observed),reader,SAVED,owner.fence,true,Fixtures.MAPPER);
            assertThat(actions).singleElement().isInstanceOf(SchemaEvolution.Create.class);
            CdcTable row=row(store);
            assertThat(row.schemaState).isEqualTo(SchemaEvolution.REFRESHING);assertThat(row.refreshStartedAt).isNotNull();
            assertThat(row.schemaDetail).contains("Floor").contains("varchar(10)");

            var pipeline=new TenantPipeline(new CdcProperties(new CdcProperties.Broker("vm://0","u","p"),List.of(Fixtures.TENANT),null,null,365,500),Fixtures.TENANT,Fixtures.MAPPER);
            var control=mock(SqlServerControl.class);
            doThrow(new SQLException("not authorized","42000",22902)).when(control).createCaptureInstance(anyString(),anyString(),any(),anyString());
            // The creation above was recorded as attempted; the pipeline replans and now gets nothing new to do.
            pipeline.evolveSchemas(store,control,registry,List.of(observed),SAVED,owner.fence);
            verify(control,never()).createCaptureInstance(anyString(),anyString(),any(),anyString());
            // An Admin request retries; a missing db_owner permission is reported instead of stopping capture.
            store.transaction(em -> {em.find(CdcTable.class,row.tableId).refreshRequestedAt=Instant.now();return null;});
            pipeline.evolveSchemas(store,control,registry,List.of(observed),SAVED,owner.fence);
            verify(control).createCaptureInstance(eq("dbo"),eq("HR_EmployeeContractInfo"),any(),startsWith("dbo_HR_EmployeeContractInfo_"));
            assertThat(row(store).refreshError).isEqualTo("CDC_MANAGEMENT_PERMISSION_REQUIRED");
            assertThat(row(store).schemaState).isEqualTo(SchemaEvolution.REFRESH_FAILED);
            assertThat(row(store).refreshRequestedAt).isNull();
            assertThat(registry.hasFailedRefresh()).isTrue();
            assertThat(registry.planSchemaEvolution(List.of(observed),reader,SAVED,owner.fence,true,Fixtures.MAPPER)).isEmpty();
            assertThat(row(store).schemaState).isEqualTo(SchemaEvolution.REFRESH_FAILED);
        }
    }
    static CdcTable row(com.iqhr.cdc.store.TenantStore store) {
        return store.transaction(em -> em.find(CdcTable.class,TableRegistry.id(Fixtures.TENANT,"dbo","HR_EmployeeContractInfo")));
    }
}
