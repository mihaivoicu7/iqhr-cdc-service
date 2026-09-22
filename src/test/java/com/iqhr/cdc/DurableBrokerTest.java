package com.iqhr.cdc;

import static org.assertj.core.api.Assertions.*;
import com.iqhr.cdc.broker.DurableBroker;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.apache.activemq.artemis.api.core.*;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.*;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;

class DurableBrokerTest {
    @TempDir Path directory;
    @Test void durableMessageSurvivesRollbackAndBrokerRestart() throws Exception {
        var config=new ConfigurationImpl().setPersistenceEnabled(true).setSecurityEnabled(false)
                .setJournalType(JournalType.NIO).setJournalDirectory(directory.resolve("journal").toString())
                .setBindingsDirectory(directory.resolve("bindings").toString()).setPagingDirectory(directory.resolve("paging").toString())
                .setLargeMessagesDirectory(directory.resolve("large").toString()).addAcceptorConfiguration("in-vm","vm://0");
        config.addAddressSetting("iqhr.cdc.#",new AddressSettings().setMaxDeliveryAttempts(-1).setExpiryDelay(-1L).setAutoCreateAddresses(false).setAutoCreateQueues(false));
        ActiveMQServer server=ActiveMQServers.newActiveMQServer(config);server.start();
        var settings=new CdcProperties.Broker("vm://0","test","test");
        try {
            try(var missing=new DurableBroker(settings,Fixtures.TENANT)) {
                assertThatThrownBy(missing::verifyQueue).hasMessage("DURABLE_BROKER_QUEUE_INVALID_OR_MISSING");
            }
            server.addAddressInfo(new AddressInfo(SimpleString.of(Fixtures.TENANT.address()),RoutingType.MULTICAST));
            server.createQueue(QueueConfiguration.of(Fixtures.TENANT.queue()).setAddress(Fixtures.TENANT.address())
                    .setRoutingType(RoutingType.MULTICAST).setDurable(true).setExclusive(true).setAutoDelete(false));
            try(var broker=new DurableBroker(settings,Fixtures.TENANT)) {
                assertThat(broker.verifyQueue()).isZero();
                try(var publisher=broker.publisher()) {publisher.publish("durable event","event-1");}
                try(var consumer=broker.consumer()) {assertThat(consumer.receive()).isEqualTo("durable event");consumer.rollback();}
            }
            server.stop();server=ActiveMQServers.newActiveMQServer(config);server.start();
            try(var broker=new DurableBroker(settings,Fixtures.TENANT);var consumer=broker.consumer()) {
                assertThat(consumer.receive()).isEqualTo("durable event");consumer.commit();
                assertThat(consumer.receive()).isNull();
            }
        } finally {server.stop();}
    }
}
