package com.iqhr.cdc.source;

import com.iqhr.cdc.model.HistoryEvent;
import io.debezium.engine.*;
import java.util.List;
import java.util.function.Consumer;

/** A failure never marks that record processed, including a failed/ambiguous broker commit. */
public final class ConfirmedChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String,String>> {
    @FunctionalInterface public interface Publisher { void publish(String body,String eventId) throws Exception; }
    private final EventCodec codec;
    private final Publisher publisher;
    private final Runnable ownershipCheck;
    private final Consumer<HistoryEvent> progress;
    public ConfirmedChangeConsumer(EventCodec codec,Publisher publisher,Runnable ownershipCheck,Consumer<HistoryEvent> progress) {
        this.codec=codec; this.publisher=publisher; this.ownershipCheck=ownershipCheck; this.progress=progress;
    }
    @Override public void handleBatch(List<ChangeEvent<String,String>> records,
            DebeziumEngine.RecordCommitter<ChangeEvent<String,String>> committer) throws InterruptedException {
        for(ChangeEvent<String,String> record:records) {
            if(Thread.currentThread().isInterrupted())throw new InterruptedException();
            ownershipCheck.run();
            HistoryEvent event=codec.parse(record.key(),record.value(),record.destination());
            if(event!=null) {
                try { publisher.publish(codec.encode(event),event.eventId); }
                catch(Exception e) { throw new IllegalStateException("BROKER_PUBLISH_UNCONFIRMED"); }
                progress.accept(event);
            }
            committer.markProcessed(record);
        }
        committer.markBatchFinished();
    }
}
