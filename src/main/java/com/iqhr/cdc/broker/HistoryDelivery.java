package com.iqhr.cdc.broker;

import com.iqhr.cdc.model.HistoryEvent;
import com.iqhr.cdc.source.EventCodec;
import java.util.function.Consumer;

public final class HistoryDelivery {
    @FunctionalInterface public interface Acknowledger { void commit() throws Exception; }
    private HistoryDelivery() {}
    public static HistoryEvent deliver(String json,EventCodec codec,Consumer<HistoryEvent> committedWrite,Acknowledger ack) throws Exception {
        HistoryEvent event=codec.decode(json);
        committedWrite.accept(event); // Returns only after the JPA transaction commits.
        ack.commit(); // Crash/ack failure now redelivers; event_id makes that a no-op.
        return event;
    }
}
