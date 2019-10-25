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

import org.junit.Test;
import org.messaginghub.amqperative.support.AMQPerativeTestSupport;
import org.messaginghub.amqperative.support.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test connections can be established to remote peers via WebSockets
 */
public class WSConnectionTest extends AMQPerativeTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(WSConnectionTest.class);

    @Override
    protected boolean isAddWebSocketConnector() {
        return true;
    }

    @Test(timeout = 60000)
    public void testCreateWebSocketConnection() throws Exception {
        URI brokerURI = getBrokerWebSocketConnectionURI();

        ClientOptions options = new ClientOptions();
        options.setContainerId(UUID.randomUUID().toString());
        Client client = Client.create(options);
        assertNotNull(client);

        ConnectionOptions connectOpts = new ConnectionOptions();
        connectOpts.transportOptions().setUseWebSockets(true);

        Connection connection = client.connect(brokerURI.getHost(), brokerURI.getPort(), connectOpts);
        assertNotNull(connection);
        assertSame(connection, connection.openFuture().get(5, TimeUnit.SECONDS));

        Wait.assertTrue("Broker did not register a connection", () -> getProxyToBroker().getCurrentConnectionsCount() == 1);

        assertSame(connection, connection.close().get(5, TimeUnit.SECONDS));

        Wait.assertTrue("Broker did not register a connection close", () -> getProxyToBroker().getCurrentConnectionsCount() == 0);
    }

    @Test(timeout = 60000)
    public void testCreateConnectionWithUserAndPassWithPlainOnlyAllowed() throws Exception {
        URI brokerURI = getBrokerWebSocketConnectionURI();

        ClientOptions options = new ClientOptions();
        options.setContainerId(UUID.randomUUID().toString());
        Client client = Client.create(options);
        assertNotNull(client);

        ConnectionOptions connectionOpts = new ConnectionOptions();
        connectionOpts.transportOptions().setUseWebSockets(true);
        connectionOpts.setUser("system");
        connectionOpts.setPassword("manager");
        connectionOpts.addAllowedMechanism("PLAIN");

        Connection connection = client.connect(brokerURI.getHost(), brokerURI.getPort(), connectionOpts);
        assertNotNull(connection);
        assertSame(connection, connection.openFuture().get(50, TimeUnit.SECONDS));

        Wait.assertTrue("Broker did not register a connection", () -> getProxyToBroker().getCurrentConnectionsCount() == 1);

        assertSame(connection, connection.close().get(5, TimeUnit.SECONDS));

        Wait.assertTrue("Broker did not register a connection close", () -> getProxyToBroker().getCurrentConnectionsCount() == 0);
    }
}
