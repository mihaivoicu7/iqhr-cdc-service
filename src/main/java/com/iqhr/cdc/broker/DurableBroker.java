package com.iqhr.cdc.broker;

import com.iqhr.cdc.CdcProperties;
import com.iqhr.cdc.CdcProperties.Tenant;
import com.iqhr.cdc.store.ContinuityException;
import jakarta.jms.*;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public final class DurableBroker implements AutoCloseable {
    private final ActiveMQConnectionFactory factory;
    private final CdcProperties.Broker broker;
    private final Tenant tenant;
    public DurableBroker(CdcProperties.Broker broker,Tenant tenant) {
        this.broker=broker; this.tenant=tenant;
        factory=new ActiveMQConnectionFactory(broker.url());
        factory.setBlockOnDurableSend(true); factory.setBlockOnAcknowledge(true);
        factory.setConsumerWindowSize(0); factory.setConfirmationWindowSize(1024*1024);
        factory.setCallTimeout(30000); factory.setCallFailoverTimeout(30000);
        factory.setInitialConnectAttempts(1); factory.setReconnectAttempts(0);
    }
    /** Read-only broker query: never silently creates a new queue after state was lost. */
    public long verifyQueue() throws Exception {
        try(ClientSessionFactory sf=factory.getServerLocator().createSessionFactory();
            ClientSession session=sf.createSession(broker.user(),broker.password(),false,true,true,false,0)) {
            ClientSession.QueueQuery queue=session.queueQuery(SimpleString.of(tenant.queue()));
            if(!queue.isExists()||!queue.isDurable()||queue.isTemporary()||queue.isPurgeOnNoConsumers()
                    ||queue.getRoutingType()!=RoutingType.MULTICAST||!tenant.address().equals(queue.getAddress().toString())
                    ||Boolean.TRUE.equals(queue.isAutoDelete())||!Boolean.TRUE.equals(queue.isExclusive())
                    ||Boolean.TRUE.equals(queue.isLastValue())||queue.getFilterString()!=null
                    ||(queue.getRingSize()!=null&&queue.getRingSize()!=-1))
                throw new ContinuityException("DURABLE_BROKER_QUEUE_INVALID_OR_MISSING");
            return queue.getMessageCount();
        }
    }
    public Publisher publisher() throws JMSException { return new Publisher(); }
    public Consumer consumer() throws JMSException { return new Consumer(); }
    public final class Publisher implements AutoCloseable {
        private final Connection connection;
        private final Session session;
        private final MessageProducer producer;
        private Publisher() throws JMSException {
            connection=factory.createConnection(broker.user(),broker.password());
            session=connection.createSession(true,Session.SESSION_TRANSACTED);
            producer=session.createProducer(session.createTopic(tenant.address()));
            producer.setDeliveryMode(DeliveryMode.PERSISTENT); producer.setTimeToLive(0);
        }
        public synchronized void publish(String body,String eventId) throws JMSException {
            TextMessage message=session.createTextMessage(body);
            message.setStringProperty("eventId",eventId); message.setStringProperty("tenantId",tenant.id());
            producer.send(message,DeliveryMode.PERSISTENT,4,0);
            // Core JMS commit waits for server confirmation. Only then may Debezium acknowledge.
            session.commit();
        }
        @Override public synchronized void close() throws JMSException { connection.close(); }
    }
    public final class Consumer implements AutoCloseable {
        private final Connection connection;
        private final Session session;
        private final MessageConsumer consumer;
        private Consumer() throws JMSException {
            connection=factory.createConnection(broker.user(),broker.password());
            session=connection.createSession(true,Session.SESSION_TRANSACTED);
            consumer=session.createConsumer(session.createQueue(tenant.address()+"::"+tenant.queue()));
            connection.start();
        }
        public String receive() throws JMSException {
            Message message=consumer.receive(1000);
            if(message==null)return null;
            if(!(message instanceof TextMessage text))throw new ContinuityException("INVALID_BROKER_MESSAGE_TYPE");
            return text.getText();
        }
        public void commit() throws JMSException { session.commit(); }
        public void rollback() throws JMSException { session.rollback(); }
        @Override public void close() throws JMSException { connection.close(); }
    }
    @Override public void close() { factory.close(); }
}
