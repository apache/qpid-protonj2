/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.protonj2.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.protonj2.client.support.ImperativeClientTestSupport;
import org.apache.qpid.protonj2.client.support.Wait;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
public class ReceiverTest extends ImperativeClientTestSupport {

    @Test
    public void testCreateReceiver() throws Exception {
        URI brokerURI = getBrokerAmqpConnectionURI();

        ClientOptions options = new ClientOptions();
        options.id(UUID.randomUUID().toString());
        Client container = Client.create(options);
        assertNotNull(container);

        Connection connection = container.connect(brokerURI.getHost(), brokerURI.getPort());
        assertNotNull(connection);
        assertSame(connection, connection.openFuture().get(5, TimeUnit.SECONDS));

        Receiver receiver = connection.openReceiver(getTestName());
        assertNotNull(receiver);
        assertSame(receiver, receiver.openFuture().get(5, TimeUnit.SECONDS));

        Wait.assertTrue("Broker did not register a receiver open", () -> getProxyToBroker().getQueueSubscribers().length == 1);

        assertSame(receiver, receiver.close().get(5, TimeUnit.SECONDS));

        Wait.assertTrue("Broker did not register a receiver close", () -> getProxyToBroker().getQueueSubscribers().length == 0);

        assertSame(connection, connection.close().get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testReceiveJMSTextMessageFromQueue() throws Exception {
        URI brokerURI = getBrokerAmqpConnectionURI();

        sendTextMessageToQueue();

        ClientOptions options = new ClientOptions();
        options.id(UUID.randomUUID().toString());
        Client container = Client.create(options);
        assertNotNull(container);

        Connection connection = container.connect(brokerURI.getHost(), brokerURI.getPort());
        assertNotNull(connection.openFuture().get(5, TimeUnit.SECONDS));
        Receiver receiver = connection.openReceiver(getTestName());
        assertSame(receiver, receiver.openFuture().get(5, TimeUnit.SECONDS));

        Wait.assertEquals(1, () -> receiver.prefetchedCount());
        Delivery delivery = receiver.receive();
        assertNotNull(delivery);
        Message<?> received = delivery.message();
        assertNotNull(received);
        assertTrue(received.body() instanceof String);
        String value = (String) received.body();
        assertEquals("Hello World", value);

        delivery.accept();

        assertSame(receiver, receiver.close().get(5, TimeUnit.SECONDS));
        assertSame(connection, connection.close().get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testReceiveJMSTextMessageFromQueueAndSettleWithDeliveryState() throws Exception {
        doTestReceiveJMSTextMessageFromQueueAndApplyDeliveryState(DeliveryState.accepted());
    }

    // TODO - Apply other dispositions

    public void doTestReceiveJMSTextMessageFromQueueAndApplyDeliveryState(DeliveryState state) throws Exception {
        URI brokerURI = getBrokerAmqpConnectionURI();

        sendTextMessageToQueue();

        ClientOptions options = new ClientOptions();
        options.id(UUID.randomUUID().toString());
        Client container = Client.create(options);
        assertNotNull(container);

        Connection connection = container.connect(brokerURI.getHost(), brokerURI.getPort());
        assertNotNull(connection.openFuture().get(5, TimeUnit.SECONDS));
        Receiver receiver = connection.openReceiver(getTestName(), new ReceiverOptions().autoAccept(false));
        assertSame(receiver, receiver.openFuture().get(5, TimeUnit.SECONDS));

        Wait.assertEquals(1, () -> receiver.prefetchedCount());
        Delivery delivery = receiver.receive();
        assertNotNull(delivery);
        Message<?> received = delivery.message();
        assertNotNull(received);
        assertTrue(received.body() instanceof String);
        String value = (String) received.body();
        assertEquals("Hello World", value);

        final QueueViewMBean queueView = getProxyToQueue(getTestName());
        Wait.assertEquals(1, () -> queueView.getInFlightCount());

        delivery.disposition(state, true);

        Wait.assertEquals(0, () -> queueView.getInFlightCount());
        if (state.getType().equals(DeliveryState.Type.ACCEPTED)) {
            Wait.assertEquals(0, () -> queueView.getQueueSize());
        }

        assertSame(receiver, receiver.close().get(5, TimeUnit.SECONDS));
        assertSame(connection, connection.close().get(5, TimeUnit.SECONDS));
    }

    private void sendTextMessageToQueue() throws Exception {
        javax.jms.Connection connection = createActiveMQConnection();
        Session session = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getTestName());
        MessageProducer producer = session.createProducer(queue);
        TextMessage message = session.createTextMessage("Hello World");
        producer.send(message);
        connection.close();
    }
}
