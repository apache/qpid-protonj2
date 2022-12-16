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
package org.apache.qpid.protonj2.client.transport.netty5;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.qpid.protonj2.client.TransportOptions;
import org.apache.qpid.protonj2.client.transport.Transport;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the WebSocketTransport with channel level security enabled.
 */
public class SecureWebSocketTransportTest extends SslTransportTest {

    private static final Logger LOG = LoggerFactory.getLogger(SecureWebSocketTransportTest.class);

    @Test
    public void testEnsureNettyIOContextCreatesWebSocketTransport() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            Transport transport = createTransport(createTransportOptions(), createSSLOptions());

            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
            } catch (Exception e) {
                LOG.info("Failed to connect to: {}:{} as expected.", HOSTNAME, port);
                fail("Should not have failed to connect to the server: " + HOSTNAME + ":" + port);
            }

            assertTrue(transport instanceof WebSocketTransport);
            assertTrue(transport.isConnected());

            transport.close();
        }

        assertTrue(!transportErrored);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Override
    protected TransportOptions createTransportOptions() {
        return new TransportOptions().useWebSockets(true);
    }

    @Override
    protected TransportOptions createServerTransportOptions() {
        return new TransportOptions().useWebSockets(true);
    }
}
