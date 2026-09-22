package com.iqhr.cdc;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;
import com.iqhr.cdc.source.*;
import java.util.*;

class EventCodecTest {
    @Test void compositeKeyAndIdentitySurviveReplayAndFieldOrder() {
        var codec=Fixtures.codec(); var first=Fixtures.event();
        var replay=codec.parse("{\"StartDate\":\"2026-01-01\",\"EmployeeID\":42}",Fixtures.json("c","null","{\"name\":\"A\"}"),"topic");
        assertThat(replay.eventId).isEqualTo(first.eventId);
        assertThat(codec.decode(codec.encode(first)).recordKey).contains("EmployeeID","StartDate");
        assertThat(new EventCodec(Fixtures.MAPPER,Fixtures.TENANT,"another-incarnation").identity(first)).isNotEqualTo(first.eventId);
    }
    @Test void typedChangedPropertiesDistinguishAbsentNullEmptyZeroFalseAndType() {
        var event=Fixtures.codec().parse("{\"id\":1}",Fixtures.json("u",
                "{\"unchanged\":false,\"nullToEmpty\":null,\"zeroToFalse\":0,\"absentNext\":null,\"numberToText\":12}",
                "{\"unchanged\":false,\"nullToEmpty\":\"\",\"zeroToFalse\":false,\"numberToText\":\"12\"}"),"topic");
        assertThat(event.operation).isEqualTo("UPDATE");
        assertThat(event.changedProperties).isEqualTo("[\"absentNext\",\"nullToEmpty\",\"numberToText\",\"zeroToFalse\"]");
    }
    @Test void insertDeleteAndSnapshotBoundary() {
        assertThat(Fixtures.event().operation).isEqualTo("INSERT");
        var deleted=Fixtures.codec().parse("{\"id\":1}",Fixtures.json("d","{\"x\":null}","null"),"topic");
        assertThat(deleted.operation).isEqualTo("DELETE"); assertThat(deleted.afterJson).isNull();
        assertThat(deleted.changedProperties).isEqualTo("[\"x\"]");
        assertThatThrownBy(() -> Fixtures.codec().parse("{\"id\":1}",Fixtures.json("r","null","{}"),"topic")).hasMessage("UNEXPECTED_SOURCE_OPERATION");
        assertThat(Fixtures.codec().parse(null,null,"topic")).isNull();
    }
    @Test void tenantAndSourceMismatchAreRejected() {
        assertThatThrownBy(() -> new EventCodec(Fixtures.MAPPER,Fixtures.tenant("OTHER"),Fixtures.INCARNATION).decode(Fixtures.codec().encode(Fixtures.event())))
                .hasMessage("INVALID_BROKER_EVENT");
        assertThatThrownBy(() -> Fixtures.codec().parse("{}",Fixtures.json("c","null","{}").replace("tdev_technophar","wrong_database"),"topic"))
                .hasMessage("UNEXPECTED_SOURCE_TABLE");
    }
    @Test void invalidLsnCannotBeAcceptedAsLatest() {
        assertThatThrownBy(() -> SqlServerControl.parseLsn("garbage")).hasMessage("INVALID_SOURCE_POSITION");
        assertThat(SqlServerControl.parseLsn("00000027:00000ac0:0003")).hasSize(10);
    }
    @Test void onlyExactInternalHeartbeatSourceIsFiltered() {
        String value=Fixtures.json("u","{\"tick_sequence\":1}","{\"tick_sequence\":2}").replace("HR_EmployeeContractInfo","IQHR_CdcHeartbeat");
        assertThat(Fixtures.codec().parse("{\"singleton_id\":1}",value,"topic")).isNull();
        assertThatThrownBy(() -> Fixtures.codec().parse("{\"singleton_id\":2}",value,"topic")).hasMessage("INVALID_INTERNAL_HEARTBEAT");
        assertThatThrownBy(() -> Fixtures.codec().parse("{\"singleton_id\":1}",value.replace("IQHR_CdcHeartbeat","IQHR_CdcEvent"),"topic"))
                .hasMessage("UNEXPECTED_SOURCE_TABLE");
    }
}
