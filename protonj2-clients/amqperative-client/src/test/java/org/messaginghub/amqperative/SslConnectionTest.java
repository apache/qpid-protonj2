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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.net.URI;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.apache.qpid.proton4j.amqp.driver.netty.NettyTestPeer;
import org.apache.qpid.proton4j.amqp.driver.netty.ServerOptions;
import org.apache.qpid.proton4j.amqp.security.SaslCode;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.junit.Ignore;
import org.junit.Test;
import org.messaginghub.amqperative.test.AMQPerativeTestCase;
import org.messaginghub.amqperative.transport.SslSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ssl.OpenSsl;

/**
 * Test for the Connection class
 */
public class SslConnectionTest extends AMQPerativeTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(SslConnectionTest.class);

    private static final String BROKER_JKS_KEYSTORE = "src/test/resources/broker-jks.keystore";
    private static final String BROKER_PKCS12_KEYSTORE = "src/test/resources/broker-pkcs12.keystore";
    private static final String BROKER_JKS_TRUSTSTORE = "src/test/resources/broker-jks.truststore";
    private static final String BROKER_PKCS12_TRUSTSTORE = "src/test/resources/broker-pkcs12.truststore";
    private static final String CLIENT_MULTI_KEYSTORE = "src/test/resources/client-multiple-keys-jks.keystore";
    private static final String CLIENT_JKS_TRUSTSTORE = "src/test/resources/client-jks.truststore";
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

    @Test(timeout = 20000)
    public void testCreateAndCloseSslConnectionJDK() throws Exception {
        testCreateAndCloseSslConnection(false);
    }

    @Test(timeout = 20000)
    public void testCreateAndCloseSslConnectionOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        testCreateAndCloseSslConnection(true);
    }

    private void testCreateAndCloseSslConnection(boolean openSSL) throws Exception {
        ServerOptions serverOpts = new ServerOptions();
        serverOpts.setSecure(true);
        serverOpts.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverOpts.setKeyStorePassword(PASSWORD);
        serverOpts.setVerifyHost(false);

        try (NettyTestPeer peer = new NettyTestPeer(serverOpts)) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = new ConnectionOptions();
            clientOptions.sslEnabled(true).sslOptions()
                                             .trustStoreLocation(CLIENT_JKS_TRUSTSTORE)
                                             .trustStorePassword(PASSWORD)
                                             .allowNativeSSL(openSSL);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), clientOptions);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            assertTrue(peer.hasSecureConnection());
            assertFalse(peer.isConnectionVerified());

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            LOG.info("Connect test completed normally");

            peer.expectClose().respond();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateAndCloseSslConnectionWithDefaultPortJDK() throws Exception {
        testCreateAndCloseSslConnectionWithDefaultPort(false);
    }

    @Test(timeout = 20000)
    public void testCreateAndCloseSslConnectionWithDefaultPortOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        testCreateAndCloseSslConnectionWithDefaultPort(true);
    }

    private void testCreateAndCloseSslConnectionWithDefaultPort(boolean openSSL) throws Exception {
        ServerOptions serverOpts = new ServerOptions();
        serverOpts.setSecure(true);
        serverOpts.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverOpts.setKeyStorePassword(PASSWORD);
        serverOpts.setVerifyHost(false);

        try (NettyTestPeer peer = new NettyTestPeer(serverOpts)) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = new ConnectionOptions();
            clientOptions.sslEnabled(true).sslOptions()
                                             .trustStoreLocation(CLIENT_JKS_TRUSTSTORE)
                                             .trustStorePassword(PASSWORD)
                                             .allowNativeSSL(openSSL)
                                             .defaultSslPort(peer.getServerURI().getPort());

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), clientOptions);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            assertTrue(peer.hasSecureConnection());
            assertFalse(peer.isConnectionVerified());

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            LOG.info("Connect test completed normally");

            peer.expectClose().respond();
            connection.close();
        }
    }

    @Ignore("Test driver not handling the preemptive header currently")
    @Test(timeout = 20000)
    public void testCreateSslConnectionWithServerSendingPreemptiveDataJDK() throws Exception {
        doTestCreateSslConnectionWithServerSendingPreemptiveData(false);
    }

    @Ignore("Test driver not handling the preemptive header currently")
    @Test(timeout = 20000)
    public void testCreateSslConnectionWithServerSendingPreemptiveDataOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        doTestCreateSslConnectionWithServerSendingPreemptiveData(true);
    }

    private void doTestCreateSslConnectionWithServerSendingPreemptiveData(boolean openSSL) throws Exception {
        ServerOptions serverOpts = new ServerOptions();
        serverOpts.setSecure(true);
        serverOpts.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverOpts.setKeyStorePassword(PASSWORD);
        serverOpts.setVerifyHost(false);

        try (NettyTestPeer peer = new NettyTestPeer(serverOpts)) {

            peer.remoteHeader(AMQPHeader.getSASLHeader()).queue();
            peer.expectSASLHeader();
            peer.remoteSaslMechanisms().withMechanisms("ANONYMOUS").queue();
            peer.expectSaslInit().withMechanism("ANONYMOUS");
            peer.remoteSaslOutcome().withCode(SaslCode.OK).queue();
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = new ConnectionOptions();
            clientOptions.sslEnabled(true).sslOptions()
                                             .trustStoreLocation(CLIENT_JKS_TRUSTSTORE)
                                             .trustStorePassword(PASSWORD)
                                             .allowNativeSSL(openSSL);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), clientOptions);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            assertTrue(peer.hasSecureConnection());

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20000)
    public void testCreateAndCloseSslConnectionWithClientAuthJDK() throws Exception {
        doTestCreateAndCloseSslConnectionWithClientAuth(false);
    }

    @Test(timeout = 20000)
    public void testCreateAndCloseSslConnectionWithClientAuthOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        doTestCreateAndCloseSslConnectionWithClientAuth(true);
    }

    private void doTestCreateAndCloseSslConnectionWithClientAuth(boolean openSSL) throws Exception {
        ServerOptions serverOpts = new ServerOptions();
        serverOpts.setSecure(true);
        serverOpts.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverOpts.setKeyStorePassword(PASSWORD);
        serverOpts.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        serverOpts.setTrustStorePassword(PASSWORD);
        serverOpts.setNeedClientAuth(true);
        serverOpts.setVerifyHost(false);

        try (NettyTestPeer peer = new NettyTestPeer(serverOpts)) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = new ConnectionOptions();
            clientOptions.sslEnabled(true).sslOptions()
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

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20000)
    public void testCreateAndCloseSslConnectionWithAliasJDK() throws Exception {
        doConnectionWithAliasTestImpl(CLIENT_KEY_ALIAS, CLIENT_DN, false);
        doConnectionWithAliasTestImpl(CLIENT2_KEY_ALIAS, CLIENT2_DN, false);
    }

    @Test(timeout = 20000)
    public void testCreateAndCloseSslConnectionWithAliasOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        doConnectionWithAliasTestImpl(CLIENT_KEY_ALIAS, CLIENT_DN, true);
        doConnectionWithAliasTestImpl(CLIENT2_KEY_ALIAS, CLIENT2_DN, true);
    }

    private void doConnectionWithAliasTestImpl(String alias, String expectedDN, boolean requestOpenSSL) throws Exception, SSLPeerUnverifiedException, IOException {
        ServerOptions serverOpts = new ServerOptions();
        serverOpts.setSecure(true);
        serverOpts.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverOpts.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        serverOpts.setKeyStorePassword(PASSWORD);
        serverOpts.setTrustStorePassword(PASSWORD);
        serverOpts.setVerifyHost(false);
        serverOpts.setNeedClientAuth(true);

        try (NettyTestPeer peer = new NettyTestPeer(serverOpts)) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = new ConnectionOptions();
            clientOptions.sslEnabled(true).sslOptions()
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
            assertEquals("Unexpected certificate DN", expectedDN, dn);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20000)
    public void testCreateConnectionWithAliasThatDoesNotExist() throws Exception {
        doCreateConnectionWithInvalidAliasTestImpl(ALIAS_DOES_NOT_EXIST);
    }

    @Test(timeout = 20000)
    public void testCreateConnectionWithAliasThatDoesNotRepresentKeyEntry() throws Exception {
        doCreateConnectionWithInvalidAliasTestImpl(ALIAS_CA_CERT);
    }

    private void doCreateConnectionWithInvalidAliasTestImpl(String alias) throws Exception, IOException {
        ServerOptions serverOpts = new ServerOptions();
        serverOpts.setSecure(true);
        serverOpts.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverOpts.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        serverOpts.setKeyStorePassword(PASSWORD);
        serverOpts.setTrustStorePassword(PASSWORD);
        serverOpts.setVerifyHost(false);
        serverOpts.setNeedClientAuth(true);

        try (NettyTestPeer peer = new NettyTestPeer(serverOpts)) {
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = new ConnectionOptions();
            clientOptions.sslEnabled(true).sslOptions()
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

            assertTrue("Attempt should have failed locally, peer should not have accepted any TCP connection", peer.isAcceptingConnections());
        }
    }

    /**
     * Checks that configuring different SSLContext instances using different client key
     * stores via {@link SslOptions#sslContextOverride(javax.net.ssl.SSLContext)} results
     * in different certificates being observed server side following handshake.
     *
     * @throws Exception if an unexpected error is encountered
     */
    @Test(timeout = 20000)
    public void testCreateConnectionWithSslContextOverride() throws Exception {
        assertNotEquals(CLIENT_JKS_KEYSTORE, CLIENT2_JKS_KEYSTORE);
        assertNotEquals(CLIENT_DN, CLIENT2_DN);

        // Connect providing the Client 1 details via context override, expect Client1 DN.
        doConnectionWithSslContextOverride(CLIENT_JKS_KEYSTORE, CLIENT_DN);
        // Connect providing the Client 2 details via context override, expect Client2 DN instead.
        doConnectionWithSslContextOverride(CLIENT2_JKS_KEYSTORE, CLIENT2_DN);
    }

    private void doConnectionWithSslContextOverride(String clientKeyStorePath, String expectedDN) throws Exception {
        ServerOptions serverOpts = new ServerOptions();
        serverOpts.setSecure(true);
        serverOpts.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverOpts.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        serverOpts.setKeyStorePassword(PASSWORD);
        serverOpts.setTrustStorePassword(PASSWORD);
        serverOpts.setNeedClientAuth(true);
        serverOpts.setVerifyHost(false);

        SslOptions clientSslOptions = new SslOptions();
        clientSslOptions.sslEnabled(true)
                        .keyStoreLocation(clientKeyStorePath)
                        .keyStorePassword(PASSWORD)
                        .trustStoreLocation(CLIENT_JKS_TRUSTSTORE)
                        .trustStorePassword(PASSWORD);

        try (NettyTestPeer peer = new NettyTestPeer(serverOpts)) {
            peer.expectSASLPlainConnect("guest", "guest");
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            SSLContext sslContext = SslSupport.createJdkSslContext(clientSslOptions);
            ConnectionOptions clientOptions = new ConnectionOptions();
            clientOptions.user("guest")
                         .password("guest")
                         .sslOptions().sslEnabled(true)
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
            assertEquals("Unexpected certificate DN", expectedDN, dn);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 20000)
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

    @Test(timeout = 20000)
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
        ServerOptions serverOptions = new ServerOptions();
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

        try (NettyTestPeer peer = new NettyTestPeer(serverOptions)) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            Client container = Client.create();
            ConnectionOptions clientOptions = new ConnectionOptions();
            clientOptions.sslOptions().sslEnabled(true);
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
            assertEquals("Unexpected certificate DN", expectedDN, dn);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
