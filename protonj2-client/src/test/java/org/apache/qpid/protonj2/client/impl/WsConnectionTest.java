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
package org.apache.qpid.protonj2.client.impl;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.concurrent.ExecutionException;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.test.driver.ProtonTestServerOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for the Connection class connecting over WebSockets
 *
 * TODO: Have this just extend the ConnectionTest and make both client and server use WS
 */
@Timeout(20)
public class WsConnectionTest extends ConnectionTest {

    private static final Logger LOG = LoggerFactory.getLogger(WsConnectionTest.class);

    @Override
    protected ProtonTestServerOptions testServerOptions() {
        return new ProtonTestServerOptions().setUseWebSockets(true);
    }

    @Override
    protected ConnectionOptions connectionOptions() {
        ConnectionOptions options = new ConnectionOptions();
        options.transportOptions().useWebSockets(true);

        return options;
    }

    @Override
    protected ConnectionOptions connectionOptions(String user, String password) {
        ConnectionOptions options = new ConnectionOptions();
        options.transportOptions().useWebSockets(true);
        options.user(user);
        options.password(password);

        return options;
    }

    @Test
    public void testWSConnectFailsDueToServerListeningOverTCP() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(testServerOptions())) {
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("WebSocket Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = connectionOptions();

            try {
                Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
                connection.openFuture().get();
                fail("Should fail to connect");
            } catch (ExecutionException ex) {
                LOG.info("Connection create failed due to: ", ex);
                assertTrue(ex.getCause() instanceof ClientException);
            }

            peer.waitForScriptToCompleteIgnoreErrors();
        }
    }
}
