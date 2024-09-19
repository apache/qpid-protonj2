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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.URI;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.SslOptions;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.client.transport.netty4.SslSupport;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.test.driver.ProtonTestServerOptions;
import org.apache.qpid.protonj2.types.security.SaslCode;
import org.apache.qpid.protonj2.types.transport.AMQPHeader;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ssl.OpenSsl;

/**
 * Test for the Connection class
 */
@Timeout(30)
public class SslConnectionTest extends ImperativeClientTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(SslConnectionTest.class);

    private static final String BROKER_JKS_KEYSTORE = "src/test/resources/broker-jks.keystore";
    private static final String BROKER_PKCS12_KEYSTORE = "src/test/resources/broker-pkcs12.keystore";
    private static final String BROKER_JKS_TRUSTSTORE = "src/test/resources/broker-jks.truststore";
    private static final String BROKER_PKCS12_TRUSTSTORE = "src/test/resources/broker-pkcs12.truststore";
    private static final String CLIENT_MULTI_KEYSTORE = "src/test/resources/client-multiple-keys-jks.keystore";
    private static final String CLIENT_JKS_TRUSTSTORE = "src/test/resources/client-jks.truststore";
    private static final String CLIENT_JKS_TRUSTSTORE_CLASSPATH = "classpath:client-jks.truststore";
    private static final String CLIENT_PKCS12_TRUSTSTORE = "src/test/resources/client-pkcs12.truststore";
    private static final String OTHER_CA_TRUSTSTORE = "src/test/resources/other-ca-jks.truststore";
    private static final String CLIENT_JKS_KEYSTORE = "src/test/resources/client-jks.keystore";
    private static final String CLIENT_PKCS12_KEYSTORE = "src/test/resources/client-pkcs12.keystore";
    private static final String CLIENT2_JKS_KEYSTORE = "src/test/resources/client2-jks.keystore";
    private static final String CUSTOM_STORE_TYPE_PKCS12 = "pkcs12";
    private static final String PASSWORD = "password";
    private static final String WRONG_PASSWORD = "wrong-password";

    private static final String CLIENT_KEY_ALIAS = "client";
    private static final String CLIENT_DN = "O=Client,CN=client";
    private static final String CLIENT2_KEY_ALIAS = "client2";
    private static final String CLIENT2_DN = "O=Client2,CN=client2";

    private static final String ALIAS_DOES_NOT_EXIST = "alias.does.not.exist";
    private static final String ALIAS_CA_CERT = "ca";

    private static final String JAVAX_NET_SSL_KEY_STORE = "javax.net.ssl.keyStore";
    private static final String JAVAX_NET_SSL_KEY_STORE_TYPE = "javax.net.ssl.keyStoreType";
    private static final String JAVAX_NET_SSL_KEY_STORE_PASSWORD = "javax.net.ssl.keyStorePassword";
    private static final String JAVAX_NET_SSL_TRUST_STORE = "javax.net.ssl.trustStore";
    private static final String JAVAX_NET_SSL_TRUST_STORE_TYPE = "javax.net.ssl.trustStoreType";
    private static final String JAVAX_NET_SSL_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";

    protected ProtonTestServerOptions serverOptions() {
        return new ProtonTestServerOptions();
    }

    protected ConnectionOptions connectionOptions() {
        return new ConnectionOptions().sslEnabled(true);
    }

    @Test
    public void testCreateAndCloseSslConnectionJDK() throws Exception {
        testCreateAndCloseSslConnection(false, false);
    }

    @Test
    public void testCreateAndCloseSslConnectionJDKTrustStoreOnClasspath() throws Exception {
        testCreateAndCloseSslConnection(false, true);
    }

    @Test
    public void testCreateAndCloseSslConnectionOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        testCreateAndCloseSslConnection(true, false);
    }

    @Test
    public void testCreateAndCloseSslConnectionOpenSSLTrustStoreOnClasspath() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        testCreateAndCloseSslConnection(true, true);
    }

    private void testCreateAndCloseSslConnection(boolean openSSL, boolean storeFromClassPath) throws Exception {
        ProtonTestServerOptions serverOptions = serverOptions();
        serverOptions.setSecure(true);
        serverOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverOptions.setKeyStorePassword(PASSWORD);
        serverOptions.setVerifyHost(false);

        try (ProtonTestServer peer = new ProtonTestServer(serverOptions)) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            final URI remoteURI = peer.getServerURI();
            final String storeLocation = storeFromClassPath ? CLIENT_JKS_TRUSTSTORE_CLASSPATH : CLIENT_JKS_TRUSTSTORE;

            ConnectionOptions clientOptions = connectionOptions();
            clientOptions.sslOptions()
                         .trustStoreLocation(storeLocation)
                         .trustStorePassword(PASSWORD)
                         .allowNativeSSL(openSSL);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), clientOptions);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            assertTrue(peer.hasSecureConnection());
            assertFalse(peer.isConnectionVerified());

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateAndCloseSslConnectionWithDefaultPortJDK() throws Exception {
        testCreateAndCloseSslConnectionWithDefaultPort(false);
    }

    @Test
    public void testCreateAndCloseSslConnectionWithDefaultPortOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        testCreateAndCloseSslConnectionWithDefaultPort(true);
    }

    private void testCreateAndCloseSslConnectionWithDefaultPort(boolean openSSL) throws Exception {
        ProtonTestServerOptions serverOptions = serverOptions();
        serverOptions.setSecure(true);
        serverOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverOptions.setKeyStorePassword(PASSWORD);
        serverOptions.setVerifyHost(false);

        try (ProtonTestServer peer = new ProtonTestServer(serverOptions)) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = connectionOptions();
            clientOptions.sslOptions()
                         .trustStoreLocation(CLIENT_JKS_TRUSTSTORE)
                         .trustStorePassword(PASSWORD)
                         .allowNativeSSL(openSSL)
                         .defaultSslPort(peer.getServerURI().getPort());

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), clientOptions);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            assertTrue(peer.hasSecureConnection());
            assertFalse(peer.isConnectionVerified());

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete();
        }
    }

    @Disabled("Test driver not handling the preemptive header currently")
    @Test
    public void testCreateSslConnectionWithServerSendingPreemptiveDataJDK() throws Exception {
        doTestCreateSslConnectionWithServerSendingPreemptiveData(false);
    }

    @Disabled("Test driver not handling the preemptive header currently")
    @Test
    public void testCreateSslConnectionWithServerSendingPreemptiveDataOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        doTestCreateSslConnectionWithServerSendingPreemptiveData(true);
    }

    private void doTestCreateSslConnectionWithServerSendingPreemptiveData(boolean openSSL) throws Exception {
        ProtonTestServerOptions serverOptions = serverOptions();
        serverOptions.setSecure(true);
        serverOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverOptions.setKeyStorePassword(PASSWORD);
        serverOptions.setVerifyHost(false);

        try (ProtonTestServer peer = new ProtonTestServer(serverOptions)) {

            peer.remoteHeader(AMQPHeader.getSASLHeader().toArray()).queue();
            peer.expectSASLHeader();
            peer.remoteSaslMechanisms().withMechanisms("ANONYMOUS").queue();
            peer.expectSaslInit().withMechanism("ANONYMOUS");
            peer.remoteSaslOutcome().withCode(SaslCode.OK.byteValue()).queue();
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = connectionOptions();
            clientOptions.sslOptions()
                         .trustStoreLocation(CLIENT_JKS_TRUSTSTORE)
                         .trustStorePassword(PASSWORD)
                         .allowNativeSSL(openSSL);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), clientOptions);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            assertTrue(peer.hasSecureConnection());

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateAndCloseSslConnectionWithClientAuthJDK() throws Exception {
        doTestCreateAndCloseSslConnectionWithClientAuth(false);
    }

    @Test
    public void testCreateAndCloseSslConnectionWithClientAuthOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        doTestCreateAndCloseSslConnectionWithClientAuth(true);
    }

    private void doTestCreateAndCloseSslConnectionWithClientAuth(boolean openSSL) throws Exception {
        ProtonTestServerOptions serverOptions = serverOptions();
        serverOptions.setSecure(true);
        serverOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverOptions.setKeyStorePassword(PASSWORD);
        serverOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        serverOptions.setTrustStorePassword(PASSWORD);
        serverOptions.setNeedClientAuth(true);
        serverOptions.setVerifyHost(false);

        try (ProtonTestServer peer = new ProtonTestServer(serverOptions)) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = connectionOptions();
            clientOptions.sslOptions()
                         .keyStoreLocation(CLIENT_MULTI_KEYSTORE)
                         .keyStorePassword(PASSWORD)
                         .trustStoreLocation(CLIENT_JKS_TRUSTSTORE)
                         .trustStorePassword(PASSWORD)
                         .allowNativeSSL(openSSL);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), clientOptions);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            assertTrue(peer.hasSecureConnection());
            assertTrue(peer.isConnectionVerified());

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateAndCloseSslConnectionWithAliasJDK() throws Exception {
        doConnectionWithAliasTestImpl(CLIENT_KEY_ALIAS, CLIENT_DN, false);
        doConnectionWithAliasTestImpl(CLIENT2_KEY_ALIAS, CLIENT2_DN, false);
    }

    @Test
    public void testCreateAndCloseSslConnectionWithAliasOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        doConnectionWithAliasTestImpl(CLIENT_KEY_ALIAS, CLIENT_DN, true);
        doConnectionWithAliasTestImpl(CLIENT2_KEY_ALIAS, CLIENT2_DN, true);
    }

    private void doConnectionWithAliasTestImpl(String alias, String expectedDN, boolean requestOpenSSL) throws Exception, SSLPeerUnverifiedException, IOException {
        ProtonTestServerOptions serverOptions = serverOptions();
        serverOptions.setSecure(true);
        serverOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        serverOptions.setKeyStorePassword(PASSWORD);
        serverOptions.setTrustStorePassword(PASSWORD);
        serverOptions.setVerifyHost(false);
        serverOptions.setNeedClientAuth(true);

        try (ProtonTestServer peer = new ProtonTestServer(serverOptions)) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = connectionOptions();
            clientOptions.sslOptions()
                         .keyStoreLocation(CLIENT_MULTI_KEYSTORE)
                         .keyStorePassword(PASSWORD)
                         .trustStoreLocation(CLIENT_JKS_TRUSTSTORE)
                         .trustStorePassword(PASSWORD)
                         .keyAlias(alias)
                         .allowNativeSSL(requestOpenSSL);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), clientOptions);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            assertTrue(peer.hasSecureConnection());
            assertTrue(peer.isConnectionVerified());

            SSLSession session = peer.getConnectionSSLEngine().getSession();

            Certificate[] peerCertificates = session.getPeerCertificates();
            assertNotNull(peerCertificates);

            Certificate cert = peerCertificates[0];
            assertTrue(cert instanceof X509Certificate);
            String dn = ((X509Certificate) cert).getSubjectX500Principal().getName();
            assertEquals(expectedDN, dn, "Unexpected certificate DN");

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateConnectionWithAliasThatDoesNotExist() throws Exception {
        doCreateConnectionWithInvalidAliasTestImpl(ALIAS_DOES_NOT_EXIST);
    }

    @Test
    public void testCreateConnectionWithAliasThatDoesNotRepresentKeyEntry() throws Exception {
        doCreateConnectionWithInvalidAliasTestImpl(ALIAS_CA_CERT);
    }

    private void doCreateConnectionWithInvalidAliasTestImpl(String alias) throws Exception, IOException {
        ProtonTestServerOptions serverOptions = serverOptions();
        serverOptions.setSecure(true);
        serverOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        serverOptions.setKeyStorePassword(PASSWORD);
        serverOptions.setTrustStorePassword(PASSWORD);
        serverOptions.setVerifyHost(false);
        serverOptions.setNeedClientAuth(true);

        try (ProtonTestServer peer = new ProtonTestServer(serverOptions)) {
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = connectionOptions();
            clientOptions.sslOptions()
                         .keyStoreLocation(CLIENT_MULTI_KEYSTORE)
                         .keyStorePassword(PASSWORD)
                         .trustStoreLocation(CLIENT_JKS_TRUSTSTORE)
                         .trustStorePassword(PASSWORD)
                         .keyAlias(alias);

            Client container = Client.create();

            try {
                container.connect(remoteURI.getHost(), remoteURI.getPort(), clientOptions);
                fail("Should have failed to connect using invalid alias");
            } catch (Throwable clix) {
                LOG.info("Client failed to open due to error: ", clix);
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            assertTrue(peer.isAcceptingConnections(), "Attempt should have failed locally, peer should not have accepted any TCP connection");
        }
    }

    /**
     * Checks that configuring different SSLContext instances using different client key
     * stores via {@link SslOptions#sslContextOverride(javax.net.ssl.SSLContext)} results
     * in different certificates being observed server side following handshake.
     *
     * @throws Exception if an unexpected error is encountered
     */
    @Test
    public void testCreateConnectionWithSslContextOverride() throws Exception {
        assertNotEquals(CLIENT_JKS_KEYSTORE, CLIENT2_JKS_KEYSTORE);
        assertNotEquals(CLIENT_DN, CLIENT2_DN);

        // Connect providing the Client 1 details via context override, expect Client1 DN.
        doConnectionWithSslContextOverride(CLIENT_JKS_KEYSTORE, CLIENT_DN);
        // Connect providing the Client 2 details via context override, expect Client2 DN instead.
        doConnectionWithSslContextOverride(CLIENT2_JKS_KEYSTORE, CLIENT2_DN);
    }

    private void doConnectionWithSslContextOverride(String clientKeyStorePath, String expectedDN) throws Exception {
        ProtonTestServerOptions serverOptions = serverOptions();
        serverOptions.setSecure(true);
        serverOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        serverOptions.setKeyStorePassword(PASSWORD);
        serverOptions.setTrustStorePassword(PASSWORD);
        serverOptions.setNeedClientAuth(true);
        serverOptions.setVerifyHost(false);

        SslOptions clientSslOptions = new SslOptions();
        clientSslOptions.sslEnabled(true)
                        .keyStoreLocation(clientKeyStorePath)
                        .keyStorePassword(PASSWORD)
                        .trustStoreLocation(CLIENT_JKS_TRUSTSTORE)
                        .trustStorePassword(PASSWORD);

        try (ProtonTestServer peer = new ProtonTestServer(serverOptions)) {
            peer.expectSASLPlainConnect("guest", "guest");
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            SSLContext sslContext = SslSupport.createJdkSslContext(clientSslOptions);
            ConnectionOptions clientOptions = connectionOptions();
            clientOptions.user("guest")
                         .password("guest")
                         .sslOptions()
                         .sslContextOverride(sslContext);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), clientOptions);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            assertTrue(peer.hasSecureConnection());
            assertTrue(peer.isConnectionVerified());

            SSLSession session = peer.getConnectionSSLEngine().getSession();

            Certificate[] peerCertificates = session.getPeerCertificates();
            assertNotNull(peerCertificates);

            Certificate cert = peerCertificates[0];
            assertTrue(cert instanceof X509Certificate);
            String dn = ((X509Certificate) cert).getSubjectX500Principal().getName();
            assertEquals(expectedDN, dn, "Unexpected certificate DN");

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConfigureStoresWithSslSystemProperties() throws Exception {
        // Set properties and expect connection as Client1
        setSslSystemPropertiesForCurrentTest(CLIENT_JKS_KEYSTORE, PASSWORD, CLIENT_JKS_TRUSTSTORE, PASSWORD);
        doConfigureStoresWithSslSystemPropertiesTestImpl(CLIENT_DN);

        // Set properties with 'wrong ca' trust store and expect connection to fail
        setSslSystemPropertiesForCurrentTest(CLIENT_JKS_KEYSTORE, PASSWORD, OTHER_CA_TRUSTSTORE, PASSWORD);
        try {
            doConfigureStoresWithSslSystemPropertiesTestImpl(null);
            fail("Connection should have failed due to wrong CA");
        } catch (Throwable clix) {
            // Expected
        }

        // Set properties with wrong key store password and expect connection to fail
        setSslSystemPropertiesForCurrentTest(CLIENT_JKS_KEYSTORE, WRONG_PASSWORD, CLIENT_JKS_TRUSTSTORE, PASSWORD);
        try {
            doConfigureStoresWithSslSystemPropertiesTestImpl(null);
            fail("Connection should have failed due to wrong keystore password");
        } catch (Throwable jmse) {
            // Expected
        }

        // Set properties with wrong trust store password and expect connection to fail
        setSslSystemPropertiesForCurrentTest(CLIENT_JKS_KEYSTORE, PASSWORD, CLIENT_JKS_TRUSTSTORE, WRONG_PASSWORD);
        try {
            doConfigureStoresWithSslSystemPropertiesTestImpl(null);
            fail("Connection should have failed due to wrong truststore password");
        } catch (Throwable jmse) {
            // Expected
        }

        // Set properties and expect connection as Client2
        setSslSystemPropertiesForCurrentTest(CLIENT2_JKS_KEYSTORE, PASSWORD, CLIENT_JKS_TRUSTSTORE, PASSWORD);
        doConfigureStoresWithSslSystemPropertiesTestImpl(CLIENT2_DN);
    }

    @Test
    public void testConfigurePkcs12StoresWithSslSystemProperties() throws Exception {
        // Set properties and expect connection as Client1
        setSslSystemPropertiesForCurrentTest(CLIENT_PKCS12_KEYSTORE, CUSTOM_STORE_TYPE_PKCS12, PASSWORD, CLIENT_PKCS12_TRUSTSTORE, CUSTOM_STORE_TYPE_PKCS12, PASSWORD);
        doConfigureStoresWithSslSystemPropertiesTestImpl(CLIENT_DN, true);
    }

    private void setSslSystemPropertiesForCurrentTest(String keystore, String keystorePassword, String truststore, String truststorePassword) {
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE, keystore);
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, keystorePassword);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE, truststore);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, truststorePassword);
    }

    private void setSslSystemPropertiesForCurrentTest(String keystore, String keystoreType, String keystorePassword, String truststore, String truststoreType, String truststorePassword) {
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE, keystore);
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE_TYPE, keystoreType);
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, keystorePassword);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE, truststore);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE_TYPE, truststoreType);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, truststorePassword);
    }

    private void doConfigureStoresWithSslSystemPropertiesTestImpl(String expectedDN) throws Exception {
        doConfigureStoresWithSslSystemPropertiesTestImpl(expectedDN, false);
    }

    private void doConfigureStoresWithSslSystemPropertiesTestImpl(String expectedDN, boolean usePkcs12Store) throws Exception {
        ProtonTestServerOptions serverOptions = serverOptions();
        serverOptions.setSecure(true);
        serverOptions.setNeedClientAuth(true);

        if (!usePkcs12Store) {
            serverOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
            serverOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
            serverOptions.setKeyStorePassword(PASSWORD);
            serverOptions.setTrustStorePassword(PASSWORD);
            serverOptions.setVerifyHost(false);
        } else {
            serverOptions.setKeyStoreLocation(BROKER_PKCS12_KEYSTORE);
            serverOptions.setTrustStoreLocation(BROKER_PKCS12_TRUSTSTORE);
            serverOptions.setKeyStoreType(CUSTOM_STORE_TYPE_PKCS12);
            serverOptions.setTrustStoreType(CUSTOM_STORE_TYPE_PKCS12);
            serverOptions.setKeyStorePassword(PASSWORD);
            serverOptions.setTrustStorePassword(PASSWORD);
            serverOptions.setVerifyHost(false);
        }

        try (ProtonTestServer peer = new ProtonTestServer(serverOptions)) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            Client container = Client.create();
            ConnectionOptions clientOptions = connectionOptions();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), clientOptions);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            assertTrue(peer.hasSecureConnection());
            assertTrue(peer.isConnectionVerified());

            SSLSession session = peer.getConnectionSSLEngine().getSession();

            Certificate[] peerCertificates = session.getPeerCertificates();
            assertNotNull(peerCertificates);

            Certificate cert = peerCertificates[0];
            assertTrue(cert instanceof X509Certificate);
            String dn = ((X509Certificate)cert).getSubjectX500Principal().getName();
            assertEquals(expectedDN, dn, "Unexpected certificate DN");

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
