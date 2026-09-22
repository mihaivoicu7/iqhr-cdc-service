package com.iqhr.cdc;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import org.junit.jupiter.api.Test;
import com.iqhr.cdc.source.ConfirmedChangeConsumer;
import com.iqhr.cdc.broker.HistoryDelivery;
import io.debezium.engine.*;
import java.util.*;

class DeliveryBoundariesTest {
    @SuppressWarnings("unchecked")
    @Test void failedPublishDoesNotAdvanceSourceAndReplayHasSameIdentity() throws Exception {
        var event=mock(ChangeEvent.class);
        when(event.key()).thenReturn("{\"id\":1}"); when(event.value()).thenReturn(Fixtures.json("c","null","{\"v\":1}"));when(event.destination()).thenReturn("topic");
        var committer=mock(DebeziumEngine.RecordCommitter.class);
        var attempts=new ArrayList<String>();
        var failed=new ConfirmedChangeConsumer(Fixtures.codec(),(body,id) -> {attempts.add(id);throw new Exception("ambiguous commit");},() -> {},ignored -> {});
        assertThatThrownBy(() -> failed.handleBatch(List.of(event),committer)).hasMessage("BROKER_PUBLISH_UNCONFIRMED");
        verifyNoInteractions(committer);
        var order=new ArrayList<String>();
        doAnswer(call -> {order.add("ack");return null;}).when(committer).markProcessed(event);
        var healthy=new ConfirmedChangeConsumer(Fixtures.codec(),(body,id) -> {attempts.add(id);order.add("confirmed");},() -> {},ignored -> {});
        healthy.handleBatch(List.of(event),committer);
        assertThat(attempts).hasSize(2).containsOnly(attempts.getFirst()); assertThat(order).containsExactly("confirmed","ack");
        verify(committer).markBatchFinished();
    }
    @Test void databaseFailureNeverAcknowledgesAndAckFailureCanReplay() throws Exception {
        String body=Fixtures.codec().encode(Fixtures.event()); var order=new ArrayList<String>();
        assertThatThrownBy(() -> HistoryDelivery.deliver(body,Fixtures.codec(),event -> {throw new IllegalStateException("DB unavailable");},() -> order.add("ack")))
                .hasMessage("DB unavailable");
        assertThat(order).isEmpty();
        var ids=new HashSet<String>();
        assertThatThrownBy(() -> HistoryDelivery.deliver(body,Fixtures.codec(),event -> {ids.add(event.eventId);order.add("db committed");},() -> {throw new Exception("ack failed");}))
                .hasMessage("ack failed");
        HistoryDelivery.deliver(body,Fixtures.codec(),event -> ids.add(event.eventId),() -> order.add("ack"));
        assertThat(ids).hasSize(1);assertThat(order).containsExactly("db committed","ack");
    }
    @SuppressWarnings("unchecked")
    @Test void capturedHeartbeatAdvancesOnlyAfterPriorPublicRecordWasConfirmed() throws Exception {
        ChangeEvent<String,String> business=mock(ChangeEvent.class),heartbeat=mock(ChangeEvent.class);
        when(business.key()).thenReturn("{\"id\":1}");when(business.value()).thenReturn(Fixtures.json("c","null","{}"));when(business.destination()).thenReturn("topic");
        when(heartbeat.key()).thenReturn("{\"singleton_id\":1}");when(heartbeat.value()).thenReturn(Fixtures.json("u","{}","{}").replace("HR_EmployeeContractInfo","IQHR_CdcHeartbeat"));when(heartbeat.destination()).thenReturn("topic");
        var committer=mock(DebeziumEngine.RecordCommitter.class);var order=new ArrayList<String>();
        doAnswer(call -> {order.add("business ack");return null;}).when(committer).markProcessed(business);
        doAnswer(call -> {order.add("heartbeat ack");return null;}).when(committer).markProcessed(heartbeat);
        var changes=new ConfirmedChangeConsumer(Fixtures.codec(),(body,id) -> order.add("public publish confirmed"),() -> {},ignored -> {});
        changes.handleBatch(List.of(business,heartbeat),committer);
        assertThat(order).containsExactly("public publish confirmed","business ack","heartbeat ack");
        var blocked=mock(DebeziumEngine.RecordCommitter.class);
        var failed=new ConfirmedChangeConsumer(Fixtures.codec(),(body,id) -> {throw new Exception();},() -> {},ignored -> {});
        assertThatThrownBy(() -> failed.handleBatch(List.of(business,heartbeat),blocked)).hasMessage("BROKER_PUBLISH_UNCONFIRMED");
        verifyNoInteractions(blocked);
    }
}
