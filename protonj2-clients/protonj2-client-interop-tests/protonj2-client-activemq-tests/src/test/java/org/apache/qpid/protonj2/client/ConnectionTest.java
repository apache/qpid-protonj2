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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.support.ImperativeClientTestSupport;
import org.apache.qpid.protonj2.client.support.Wait;
import org.junit.Test;

/**
 * Test for basic JmsConnection functionality and error handling.
 */
public class ConnectionTest extends ImperativeClientTestSupport {

    @Test(timeout = 60000)
    public void testCreateConnection() throws Exception {
        URI brokerURI = getBrokerAmqpConnectionURI();

        ClientOptions options = new ClientOptions();
        options.id(UUID.randomUUID().toString());
        Client client = Client.create(options);
        assertNotNull(client);

        Connection connection = client.connect(brokerURI.getHost(), brokerURI.getPort());
        assertNotNull(connection);
        assertSame(connection, connection.openFuture().get(5, TimeUnit.SECONDS));

        Wait.assertTrue("Broker did not register a connection", () -> getProxyToBroker().getCurrentConnectionsCount() == 1);

        assertSame(connection, connection.close().get(5, TimeUnit.SECONDS));

        Wait.assertTrue("Broker did not register a connection close", () -> getProxyToBroker().getCurrentConnectionsCount() == 0);
    }

    @Test(timeout = 60000)
    public void testCreateConnectionWithUserAndPassWithPlainOnlyAllowed() throws Exception {
        URI brokerURI = getBrokerAmqpConnectionURI();

        ClientOptions options = new ClientOptions();
        options.id(UUID.randomUUID().toString());
        Client client = Client.create(options);
        assertNotNull(client);

        ConnectionOptions connectionOpts = new ConnectionOptions();
        connectionOpts.user("system");
        connectionOpts.password("manager");
        connectionOpts.saslOptions().addAllowedMechanism("PLAIN");

        Connection connection = client.connect(brokerURI.getHost(), brokerURI.getPort(), connectionOpts);
        assertNotNull(connection);
        assertSame(connection, connection.openFuture().get(50, TimeUnit.SECONDS));

        Wait.assertTrue("Broker did not register a connection", () -> getProxyToBroker().getCurrentConnectionsCount() == 1);

        assertSame(connection, connection.close().get(5, TimeUnit.SECONDS));

        Wait.assertTrue("Broker did not register a connection close", () -> getProxyToBroker().getCurrentConnectionsCount() == 0);
    }
}
