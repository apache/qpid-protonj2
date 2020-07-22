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
package org.apache.qpid.protonj2.client.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.SslOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test basic functionality of the Netty based TCP Transport ruuing in secure mode (SSL).
 */
@Timeout(30)
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

    @Test
    public void testConnectToServerWithoutTrustStoreFails() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            Transport transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), createSSLOptionsWithoutTrustStore(false));
            try {
                transport.connect(null);
                fail("Should have failed to connect to the server: " + HOSTNAME + ":" + port);
            } catch (Exception e) {
                LOG.info("Connection failed to untrusted test server: {}:{}", HOSTNAME, port);
            }

            assertFalse(transport.isConnected());

            transport.close();
        }

        logTransportErrors();

        assertTrue(exceptions.isEmpty());
    }

    @Test
    public void testConnectToServerUsingUntrustedKeyFails() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            SslOptions sslOptions = createSSLOptions();

            sslOptions.trustStoreLocation(OTHER_CA_TRUSTSTORE);
            sslOptions.trustStorePassword(PASSWORD);

            Transport transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), sslOptions);
            try {
                transport.connect(null);
                fail("Should have failed to connect to the server: " + HOSTNAME + ":" + port);
            } catch (Exception e) {
                LOG.info("Connection failed to untrusted test server: {}:{}", HOSTNAME, port);
            }

            assertFalse(transport.isConnected());

            transport.close();
        }
    }

    @Test
    public void testConnectToServerClientTrustsAll() throws Exception {
        try (NettyEchoServer server = createEchoServer()) {
            server.start();

            final int port = server.getServerPort();

            Transport transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), createSSLOptionsWithoutTrustStore(true));
            try {
                transport.connect(null);
                LOG.info("Connection established to test server: {}:{}", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertTrue(transport.isSecure());

            transport.close();
        }

        logTransportErrors();
        assertTrue(exceptions.isEmpty());
    }

    @Test
    public void testConnectWithNeedClientAuth() throws Exception {
        try (NettyEchoServer server = createEchoServer(true)) {
            server.start();

            final int port = server.getServerPort();

            Transport transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), createSSLOptions());
            try {
                transport.connect(null);
                LOG.info("Connection established to test server: {}:{}", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertTrue(transport.isSecure());

            // Verify there was a certificate sent to the server
            assertTrue(server.getSslHandler().handshakeFuture().await(2, TimeUnit.SECONDS), "Server handshake did not complete in alotted time");
            assertNotNull(server.getSslHandler().engine().getSession().getPeerCertificates());

            transport.close();
        }

        logTransportErrors();
        assertTrue(exceptions.isEmpty());
    }

    @Test
    public void testConnectWithSpecificClientAuthKeyAlias() throws Exception {
        doClientAuthAliasTestImpl(CLIENT_KEY_ALIAS, CLIENT_DN);
        doClientAuthAliasTestImpl(CLIENT2_KEY_ALIAS, CLIENT2_DN);
    }

    private void doClientAuthAliasTestImpl(String alias, String expectedDN) throws Exception, URISyntaxException, IOException, InterruptedException {
        try (NettyEchoServer server = createEchoServer(true)) {
            server.start();

            final int port = server.getServerPort();

            SslOptions sslOptions = createSSLOptions();
            sslOptions.keyStoreLocation(CLIENT_MULTI_KEYSTORE);
            sslOptions.keyAlias(alias);

            Transport transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), sslOptions);
            try {
                transport.connect(null);
                LOG.info("Connection established to test server: {}:{}", HOSTNAME, port);
            } catch (Exception e) {
                fail("Should not have failed to connect to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
            }

            assertTrue(transport.isConnected());
            assertTrue(transport.isSecure());

            assertTrue(server.getSslHandler().handshakeFuture().await(2, TimeUnit.SECONDS), "Server handshake did not complete in alotted time");

            Certificate[] peerCertificates = server.getSslHandler().engine().getSession().getPeerCertificates();
            assertNotNull(peerCertificates);

            Certificate cert = peerCertificates[0];
            assertTrue(cert instanceof X509Certificate);
            String dn = ((X509Certificate)cert).getSubjectX500Principal().getName();
            assertEquals(expectedDN, dn, "Unexpected certificate DN");

            transport.close();
        }

        logTransportErrors();
        assertTrue(exceptions.isEmpty());
    }

    @Test
    public void testConnectToServerVerifyHost() throws Exception {
        doConnectToServerVerifyHostTestImpl(true);
    }

    @Test
    public void testConnectToServerNoVerifyHost() throws Exception {
        doConnectToServerVerifyHostTestImpl(false);
    }

    private void doConnectToServerVerifyHostTestImpl(boolean verifyHost) throws Exception, URISyntaxException, IOException, InterruptedException {
        SslOptions serverOptions = createServerSSLOptions();
        serverOptions.keyStoreLocation(SERVER_WRONG_HOST_KEYSTORE);

        try (NettyEchoServer server = createEchoServer(serverOptions)) {
            server.start();

            final int port = server.getServerPort();

            SslOptions clientOptions = createSSLOptionsIsVerify(verifyHost);

            if (verifyHost) {
                assertTrue(clientOptions.verifyHost(), "Expected verifyHost to be true");
            } else {
                assertFalse(clientOptions.verifyHost(), "Expected verifyHost to be false");
            }

            Transport transport = createTransport(HOSTNAME, port, testListener, createTransportOptions(), clientOptions);
            try {
                transport.connect(null);
                if (verifyHost) {
                    fail("Should not have connected to the server: " + HOSTNAME + ":" + port);
                }
            } catch (Exception e) {
                if (verifyHost) {
                    LOG.info("Connection failed to test server: {}:{} as expected.", HOSTNAME, port);
                } else {
                    LOG.error("Failed to connect to test server: {}:{}" + HOSTNAME, port, e);
                    fail("Should have connected to the server at " + HOSTNAME + ":" + port + " but got exception: " + e);
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

        options.sslEnabled(true);
        options.keyStoreLocation(CLIENT_KEYSTORE);
        options.keyStorePassword(PASSWORD);
        options.trustStoreLocation(CLIENT_TRUSTSTORE);
        options.trustStorePassword(PASSWORD);
        options.storeType(KEYSTORE_TYPE);
        options.verifyHost(verifyHost);

        return options;
    }

    protected SslOptions createSSLOptionsWithoutTrustStore(boolean trustAll) {
        SslOptions options = new SslOptions();

        options.sslEnabled(true);
        options.storeType(KEYSTORE_TYPE);
        options.trustAll(trustAll);

        return options;
    }

    @Override
    protected SslOptions createServerSSLOptions() {
        SslOptions options = new SslOptions();

        // Run the server in JDK mode for now to validate cross compatibility
        options.sslEnabled(true);
        options.keyStoreLocation(SERVER_KEYSTORE);
        options.keyStorePassword(PASSWORD);
        options.trustStoreLocation(SERVER_TRUSTSTORE);
        options.trustStorePassword(PASSWORD);
        options.storeType(KEYSTORE_TYPE);
        options.verifyHost(false);

        return options;
    }
}
