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
package org.messaginghub.amqperative;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.junit.Test;
import org.messaginghub.amqperative.Connection;
import org.messaginghub.amqperative.Client;
import org.messaginghub.amqperative.ClientOptions;
import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.support.AMQPerativeTestSupport;
import org.messaginghub.amqperative.support.Wait;

public class SenderTest extends AMQPerativeTestSupport {

    @Test(timeout = 60000)
    public void testCreateReceiver() throws Exception {
        URI brokerURI = getBrokerAmqpConnectionURI();

        ClientOptions options = new ClientOptions();
        options.setContainerId(UUID.randomUUID().toString());
        Client container = Client.create(options);
        assertNotNull(container);

        Connection connection = container.connect(brokerURI.getHost(), brokerURI.getPort());
        assertNotNull(connection);
        assertSame(connection, connection.openFuture().get(5, TimeUnit.SECONDS));

        Sender sender = connection.openSender(getTestName());
        assertNotNull(sender);
        assertSame(sender, sender.openFuture().get(5, TimeUnit.SECONDS));

        Wait.assertTrue("Broker did not register a sender open", () -> getProxyToBroker().getQueueProducers().length == 1);

        assertSame(sender, sender.close().get(5, TimeUnit.SECONDS));

        Wait.assertTrue("Broker did not register a sender close", () -> getProxyToBroker().getQueueProducers().length == 0);

        assertSame(connection, connection.close().get(5, TimeUnit.SECONDS));
    }

    @Test(timeout = 60000)
    public void testSendMessageToRemoteQueueDurable() throws Exception {
        doTestSendMessageToRemoteQueueDurable(true);
    }

    @Test(timeout = 60000)
    public void testSendMessageToRemoteQueueNotDurable() throws Exception {
        doTestSendMessageToRemoteQueueDurable(false);
    }

    private void doTestSendMessageToRemoteQueueDurable(boolean durable) throws Exception {
        URI brokerURI = getBrokerAmqpConnectionURI();

        ClientOptions options = new ClientOptions();
        options.setContainerId(UUID.randomUUID().toString());
        Client container = Client.create(options);
        assertNotNull(container);

        Connection connection = container.connect(brokerURI.getHost(), brokerURI.getPort());
        assertNotNull(connection);
        assertSame(connection, connection.openFuture().get(5, TimeUnit.SECONDS));

        Sender sender = connection.openSender(getTestName()).openFuture().get(5, TimeUnit.SECONDS);
        assertNotNull(sender);

        Message<String> message = Message.create("Hello World").setDurable(durable);
        sender.send(message);

        final QueueViewMBean queueView = getProxyToQueue(getTestName());
        Wait.assertEquals(1, () -> queueView.getQueueSize());

        assertNotNull(sender.close().get(5, TimeUnit.SECONDS));
        assertNotNull(connection.close().get(5, TimeUnit.SECONDS));
    }
}
