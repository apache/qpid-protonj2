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

import javax.net.ssl.SSLContext;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.broker.TransportConnector;
import org.apache.qpid.protonj2.client.support.ImperativeClientTestSupport;
import org.apache.qpid.protonj2.client.support.Wait;
import org.apache.qpid.protonj2.client.transport.SslSupport;
import org.junit.Before;
import org.junit.Test;

/**
 * Test that we can connect to a broker over SSL.
 */
public class SSLConnectionTest extends ImperativeClientTestSupport {

    private static final String PASSWORD = "password";
    private static final String KEYSTORE = "src/test/resources/broker-jks.keystore";
    private static final String TRUSTSTORE = "src/test/resources/client-jks.truststore";

    private URI connectionURI;

    @Test(timeout = 60000)
    public void testCreateConnection() throws Exception {
        ClientOptions options = new ClientOptions();
        options.id(UUID.randomUUID().toString());
        Client client = Client.create(options);
        assertNotNull(client);

        ConnectionOptions connectOpts = new ConnectionOptions();
        connectOpts.sslEnabled(true);
        connectOpts.sslOptions().trustStoreLocation(TRUSTSTORE);
        connectOpts.sslOptions().trustStorePassword(PASSWORD);

        Connection connection = client.connect(connectionURI.getHost(), connectionURI.getPort(), connectOpts);
        assertNotNull(connection);
        assertSame(connection, connection.openFuture().get(5, TimeUnit.SECONDS));

        Wait.assertTrue("Broker did not register a connection", () -> getProxyToBroker().getCurrentConnectionsCount() == 1);

        assertSame(connection, connection.close().get(5, TimeUnit.SECONDS));

        Wait.assertTrue("Broker did not register a connection close", () -> getProxyToBroker().getCurrentConnectionsCount() == 0);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setUseJmx(true);
        brokerService.getManagementContext().setCreateConnector(false);

        // Setup broker SSL context...
        SslOptions sslOptions = new SslOptions();
        sslOptions.keyStoreLocation(KEYSTORE);
        sslOptions.keyStorePassword(PASSWORD);
        sslOptions.verifyHost(false);

        SSLContext sslContext = SslSupport.createJdkSslContext(sslOptions);

        final SslContext brokerContext = new SslContext();
        brokerContext.setSSLContext(sslContext);

        brokerService.setSslContext(brokerContext);

        TransportConnector connector = brokerService.addConnector("amqp+ssl://localhost:0");
        brokerService.start();
        brokerService.waitUntilStarted();

        connectionURI = connector.getPublishableConnectURI();
    }
}
