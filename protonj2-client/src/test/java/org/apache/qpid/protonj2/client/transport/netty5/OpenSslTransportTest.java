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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.reflect.Field;

import javax.net.ssl.SSLContext;

import org.apache.qpid.protonj2.client.SslOptions;
import org.apache.qpid.protonj2.client.transport.Transport;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty5.channel.Channel;
import io.netty5.channel.ChannelHandler;
import io.netty5.handler.ssl.OpenSsl;
import io.netty5.handler.ssl.OpenSslEngine;
import io.netty5.handler.ssl.SslHandler;

/**
 * Test basic functionality of the Netty based TCP Transport running in secure mode (SSL).
 */
@Timeout(30)
public class OpenSslTransportTest extends SslTransportTest {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSslTransportTest.class);

    @Test
    public void testConnectToServerWithOpenSSLEnabled() throws Exception {
        doTestOpenSSLSupport(true);
    }

    @Test
    public void testConnectToServerWithOpenSSLDisabled() throws Exception {
        doTestOpenSSLSupport(false);
    }

    private void doTestOpenSSLSupport(boolean useOpenSSL) throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            SslOptions options = createSSLOptions();
            options.allowNativeSSL(useOpenSSL);

            Transport transport = createTransport(createTransportOptions(), options);
            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{}:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should have connected to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertEquals(HOSTNAME, transport.getHost());
            assertEquals(port, transport.getPort());
            assertOpenSSL("Transport should be using OpenSSL", useOpenSSL, transport);

            transport.close();

            // Additional close should not fail or cause other problems.
            transport.close();
        }

        assertTrue(!transportErrored);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    @Test
    public void testConnectToServerWithUserSuppliedSSLContextWorksWhenOpenSSLRequested() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            SslOptions options = new SslOptions();

            options.sslEnabled(true);
            options.keyStoreLocation(CLIENT_KEYSTORE);
            options.keyStorePassword(PASSWORD);
            options.trustStoreLocation(CLIENT_TRUSTSTORE);
            options.trustStorePassword(PASSWORD);
            options.storeType(KEYSTORE_TYPE);

            SSLContext sslContext = SslSupport.createJdkSslContext(options);

            options = new SslOptions();
            options.sslEnabled(true);
            options.verifyHost(false);
            options.allowNativeSSL(true);
            options.sslContextOverride(sslContext);

            Transport transport = createTransport(createTransportOptions(), options);
            try {
                transport.connect(HOSTNAME, port, testListener).awaitConnect();
                LOG.info("Connected to server:{} as expected.", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should have connected to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertEquals(HOSTNAME, transport.getHost(), "Server host is incorrect");
            assertEquals(port, transport.getPort(), "Server port is incorrect");
            assertOpenSSL("Transport should not be using OpenSSL", false, transport);

            transport.close();

            // Additional close should not fail or cause other problems.
            transport.close();
        }

        assertTrue(!transportErrored);  // Normal shutdown does not trigger the event.
        assertTrue(exceptions.isEmpty());
        assertTrue(data.isEmpty());
    }

    private void assertOpenSSL(String message, boolean expected, Transport transport) throws Exception {
        Field channel = null;
        Class<?> transportType = transport.getClass();

        while (transportType != null && channel == null) {
            try {
                channel = transportType.getDeclaredField("channel");
            } catch (NoSuchFieldException error) {
                transportType = transportType.getSuperclass();
                if (Object.class.equals(transportType)) {
                    transportType = null;
                }
            }
        }

        assertNotNull(channel, "Transport implementation unknown");

        channel.setAccessible(true);

        Channel activeChannel = (Channel) channel.get(transport) ;
        ChannelHandler handler = activeChannel.pipeline().get("ssl");
        assertNotNull(handler, "Channel should have an SSL Handler registered");
        assertTrue(handler instanceof SslHandler);
        SslHandler sslHandler = (SslHandler) handler;

        if (expected) {
            assertTrue(sslHandler.engine() instanceof OpenSslEngine, message);
        } else {
            assertFalse(sslHandler.engine() instanceof OpenSslEngine, message);
        }
    }

    @Override
    @Disabled("Can't apply keyAlias in Netty OpenSSL impl")
    @Test
    public void testConnectWithSpecificClientAuthKeyAlias1() throws Exception {
        // TODO - Revert to superclass version if keyAlias becomes supported for Netty.
    }

    @Override
    @Disabled("Can't apply keyAlias in Netty OpenSSL impl")
    @Test
    public void testConnectWithSpecificClientAuthKeyAlias2() throws Exception {
        // TODO - Revert to superclass version if keyAlias becomes supported for Netty.
    }

    @Override
    protected SslOptions createSSLOptionsIsVerify(boolean verifyHost) {
        SslOptions options = new SslOptions();

        options.sslEnabled(true);
        options.allowNativeSSL(true);
        options.keyStoreLocation(CLIENT_KEYSTORE);
        options.keyStorePassword(PASSWORD);
        options.trustStoreLocation(CLIENT_TRUSTSTORE);
        options.trustStorePassword(PASSWORD);
        options.storeType(KEYSTORE_TYPE);
        options.verifyHost(verifyHost);

        return options;
    }

    @Override
    protected SslOptions createSSLOptionsWithoutTrustStore(boolean trustAll) {
        SslOptions options = new SslOptions();

        options.sslEnabled(true);
        options.storeType(KEYSTORE_TYPE);
        options.allowNativeSSL(true);
        options.trustAll(trustAll);

        return options;
    }
}
