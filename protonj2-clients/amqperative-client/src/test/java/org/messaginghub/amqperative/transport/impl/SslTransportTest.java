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
package org.messaginghub.amqperative.transport.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.messaginghub.amqperative.SslOptions;
import org.messaginghub.amqperative.TransportOptions;
import org.messaginghub.amqperative.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test basic functionality of the Netty based TCP Transport ruuing in secure mode (SSL).
 */
public class SslTransportTest extends TcpTransportTest {

    private static final Logger LOG = LoggerFactory.getLogger(SslTransportTest.class);

    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/broker-jks.keystore";
    public static final String SERVER_TRUSTSTORE = "src/test/resources/broker-jks.truststore";
    public static final String SERVER_WRONG_HOST_KEYSTORE = "src/test/resources/broker-wrong-host-jks.keystore";
    public static final String CLIENT_KEYSTORE = "src/test/resources/client-jks.keystore";
    public static final String CLIENT_MULTI_KEYSTORE = "src/test/resources/client-multiple-keys-jks.keystore";
    public static final String CLIENT_TRUSTSTORE = "src/test/resources/client-jks.truststore";
    public static final String OTHER_CA_TRUSTSTORE = "src/test/resources/other-ca-jks.truststore";

    public static final String CLIENT_KEY_ALIAS = "client";
    public static final String CLIENT_DN = "O=Client,CN=client";
    public static final String CLIENT2_KEY_ALIAS = "client2";
    public static final String CLIENT2_DN = "O=Client2,CN=client2";

    public static final String KEYSTORE_TYPE = "jks";

    @Override
    @Test(timeout = 60000)
    public void testCreateWithNullOptionsThrowsIAE() throws Exception {
        URI serverLocation = new URI("tcp://localhost:5762");

        try {
            createTransport(serverLocation, testListener, null, null);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }

        try {
            createTransport(serverLocation, testListener, new TransportOptions(), null);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }

        try {
            createTransport(serverLocation, testListener, null, new SslOptions());
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
        }
    }

    @Test(timeout = 60000)
    public void testConnectToServerWithoutTrustStoreFails() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createTransport(serverLocation, testListener, createTransportOptions(), createSSLOptionsWithoutTrustStore(false));
            try {
                transport.connect(null, null);
                fail("Should not have connected to the server: " + serverLocation);
            } catch (Exception e) {
                LOG.info("Connection failed to untrusted test server: {}", serverLocation);
            }

            assertFalse(transport.isConnected());

            transport.close();
        }

        logTransportErrors();

        assertTrue(exceptions.isEmpty());
    }

    @Test(timeout = 60000)
    public void testConnectToServerUsingUntrustedKeyFails() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            SslOptions sslOptions = createSSLOptions();

            sslOptions.setTrustStoreLocation(OTHER_CA_TRUSTSTORE);
            sslOptions.setTrustStorePassword(PASSWORD);

            Transport transport = createTransport(serverLocation, testListener, createTransportOptions(), sslOptions);
            try {
                transport.connect(null, null);
                fail("Should not have connected to the server: " + serverLocation);
            } catch (Exception e) {
                LOG.info("Connection failed to untrusted test server: {}", serverLocation);
            }

            assertFalse(transport.isConnected());

            transport.close();
        }
    }

    @Test(timeout = 60000)
    public void testConnectToServerClientTrustsAll() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createTransport(serverLocation, testListener, createTransportOptions(), createSSLOptionsWithoutTrustStore(true));
            try {
                transport.connect(null, null);
                LOG.info("Connection established to untrusted test server: {}", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server at " + serverLocation + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertTrue(transport.isSecure());

            transport.close();
        }

        logTransportErrors();
        assertTrue(exceptions.isEmpty());
    }

    @Test(timeout = 60000)
    public void testConnectWithNeedClientAuth() throws Exception {
        try (NettyEchoServer server = createEchoServer(true)) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            Transport transport = createTransport(serverLocation, testListener, createTransportOptions(), createSSLOptions());
            try {
                transport.connect(null, null);
                LOG.info("Connection established to test server: {}", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server at " + serverLocation + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertTrue(transport.isSecure());

            // Verify there was a certificate sent to the server
            assertTrue("Server handshake did not complete in alotted time", server.getSslHandler().handshakeFuture().await(2, TimeUnit.SECONDS));
            assertNotNull(server.getSslHandler().engine().getSession().getPeerCertificates());

            transport.close();
        }

        logTransportErrors();
        assertTrue(exceptions.isEmpty());
    }

    @Test(timeout = 60000)
    public void testConnectWithSpecificClientAuthKeyAlias() throws Exception {
        doClientAuthAliasTestImpl(CLIENT_KEY_ALIAS, CLIENT_DN);
        doClientAuthAliasTestImpl(CLIENT2_KEY_ALIAS, CLIENT2_DN);
    }

    private void doClientAuthAliasTestImpl(String alias, String expectedDN) throws Exception, URISyntaxException, IOException, InterruptedException {
        try (NettyEchoServer server = createEchoServer(true)) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            SslOptions sslOptions = createSSLOptions();
            sslOptions.setKeyStoreLocation(CLIENT_MULTI_KEYSTORE);
            sslOptions.setKeyAlias(alias);

            Transport transport = createTransport(serverLocation, testListener, createTransportOptions(), sslOptions);
            try {
                transport.connect(null, null);
                LOG.info("Connection established to test server: {}", serverLocation);
            } catch (Exception e) {
                fail("Should have connected to the server at " + serverLocation + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertTrue(transport.isSecure());

            assertTrue("Server handshake did not complete in alotted time", server.getSslHandler().handshakeFuture().await(2, TimeUnit.SECONDS));

            Certificate[] peerCertificates = server.getSslHandler().engine().getSession().getPeerCertificates();
            assertNotNull(peerCertificates);

            Certificate cert = peerCertificates[0];
            assertTrue(cert instanceof X509Certificate);
            String dn = ((X509Certificate)cert).getSubjectX500Principal().getName();
            assertEquals("Unexpected certificate DN", expectedDN, dn);

            transport.close();
        }

        logTransportErrors();
        assertTrue(exceptions.isEmpty());
    }

    @Test(timeout = 60000)
    public void testConnectToServerVerifyHost() throws Exception {
        doConnectToServerVerifyHostTestImpl(true);
    }

    @Test(timeout = 60000)
    public void testConnectToServerNoVerifyHost() throws Exception {
        doConnectToServerVerifyHostTestImpl(false);
    }

    private void doConnectToServerVerifyHostTestImpl(boolean verifyHost) throws Exception, URISyntaxException, IOException, InterruptedException {
        SslOptions serverOptions = createServerSSLOptions();
        serverOptions.setKeyStoreLocation(SERVER_WRONG_HOST_KEYSTORE);

        try (NettyEchoServer server = createEchoServer(serverOptions)) {
            server.start();

            int port = server.getServerPort();
            URI serverLocation = new URI("tcp://localhost:" + port);

            SslOptions clientOptions = createSSLOptionsIsVerify(verifyHost);

            if (verifyHost) {
                assertTrue("Expected verifyHost to be true", clientOptions.isVerifyHost());
            } else {
                assertFalse("Expected verifyHost to be false", clientOptions.isVerifyHost());
            }

            Transport transport = createTransport(serverLocation, testListener, createTransportOptions(), clientOptions);
            try {
                transport.connect(null, null);
                if (verifyHost) {
                    fail("Should not have connected to the server: " + serverLocation);
                }
            } catch (Exception e) {
                if (verifyHost) {
                    LOG.info("Connection failed to test server: {} as expected.", serverLocation);
                } else {
                    LOG.error("Failed to connect to test server: " + serverLocation, e);
                    fail("Should have connected to the server at " + serverLocation + " but got exception: " + e);
                }
            }

            if (verifyHost) {
                assertFalse(transport.isConnected());
            } else {
                assertTrue(transport.isConnected());
            }

            transport.close();
        }
    }

    @Override
    protected SslOptions createSSLOptions() {
        return createSSLOptionsIsVerify(false);
    }

    protected SslOptions createSSLOptionsIsVerify(boolean verifyHost) {
        SslOptions options = new SslOptions();

        options.setSSLEnabled(true);
        options.setKeyStoreLocation(CLIENT_KEYSTORE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStoreLocation(CLIENT_TRUSTSTORE);
        options.setTrustStorePassword(PASSWORD);
        options.setStoreType(KEYSTORE_TYPE);
        options.setVerifyHost(verifyHost);

        return options;
    }

    protected SslOptions createSSLOptionsWithoutTrustStore(boolean trustAll) {
        SslOptions options = new SslOptions();

        options.setSSLEnabled(true);
        options.setStoreType(KEYSTORE_TYPE);
        options.setTrustAll(trustAll);

        return options;
    }

    @Override
    protected SslOptions createServerSSLOptions() {
        SslOptions options = new SslOptions();

        // Run the server in JDK mode for now to validate cross compatibility
        options.setSSLEnabled(true);
        options.setKeyStoreLocation(SERVER_KEYSTORE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStoreLocation(SERVER_TRUSTSTORE);
        options.setTrustStorePassword(PASSWORD);
        options.setStoreType(KEYSTORE_TYPE);
        options.setVerifyHost(false);

        return options;
    }
}
