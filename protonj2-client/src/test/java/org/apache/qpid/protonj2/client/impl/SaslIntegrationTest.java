/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.protonj2.client.impl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionSecuritySaslException;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.test.driver.ProtonTestServerOptions;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(20)
public class SaslIntegrationTest extends ImperativeClientTestCase {

    private static final String ANONYMOUS = "ANONYMOUS";
    private static final String PLAIN = "PLAIN";
    private static final String CRAM_MD5 = "CRAM-MD5";
    private static final String SCRAM_SHA_1 = "SCRAM-SHA-1";
    private static final String SCRAM_SHA_256 = "SCRAM-SHA-256";
    private static final String SCRAM_SHA_512 = "SCRAM-SHA-512";
    private static final String EXTERNAL = "EXTERNAL";
    private static final String XOAUTH2 = "XOAUTH2";

    private static final UnsignedByte SASL_FAIL_AUTH = UnsignedByte.valueOf((byte) 1);
    private static final UnsignedByte SASL_SYS = UnsignedByte.valueOf((byte) 2);
    private static final UnsignedByte SASL_SYS_PERM = UnsignedByte.valueOf((byte) 3);
    private static final UnsignedByte SASL_SYS_TEMP = UnsignedByte.valueOf((byte) 4);

    private static final String BROKER_JKS_KEYSTORE = "src/test/resources/broker-jks.keystore";
    private static final String BROKER_JKS_TRUSTSTORE = "src/test/resources/broker-jks.truststore";
    private static final String CLIENT_JKS_KEYSTORE = "src/test/resources/client-jks.keystore";
    private static final String CLIENT_JKS_TRUSTSTORE = "src/test/resources/client-jks.truststore";
    private static final String PASSWORD = "password";

    protected ProtonTestServerOptions serverOptions() {
        return new ProtonTestServerOptions();
    }

    protected ConnectionOptions connectionOptions() {
        return new ConnectionOptions();
    }

    @Test
    public void testSaslExternalConnection() throws Exception {
        ProtonTestServerOptions serverOptions = serverOptions();
        serverOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverOptions.setKeyStorePassword(PASSWORD);
        serverOptions.setVerifyHost(false);
        serverOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
        serverOptions.setTrustStorePassword(PASSWORD);
        serverOptions.setNeedClientAuth(true);
        serverOptions.setSecure(true);

        try (ProtonTestServer peer = new ProtonTestServer(serverOptions)) {
            peer.expectSaslExternalConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = connectionOptions();
            clientOptions.sslOptions()
                         .sslEnabled(true)
                         .keyStoreLocation(CLIENT_JKS_KEYSTORE)
                         .keyStorePassword(PASSWORD)
                         .trustStoreLocation(CLIENT_JKS_TRUSTSTORE)
                         .trustStorePassword(PASSWORD);

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
    public void testSaslPlainConnection() throws Exception {
        final String username = "user";
        final String password = "qwerty123456";

        try (ProtonTestServer peer = new ProtonTestServer(serverOptions())) {
            peer.expectSASLPlainConnect(username, password);
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = connectionOptions();
            clientOptions.user(username);
            clientOptions.password(password);
            clientOptions.traceFrames(true);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), clientOptions);

            connection.openFuture().get(10, TimeUnit.SECONDS);

            assertFalse(peer.hasSecureConnection());
            assertFalse(peer.isConnectionVerified());

            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSaslXOauth2Connection() throws Exception {
        final String username = "user";
        final String password = "eyB1c2VyPSJ1c2VyIiB9";

        try (ProtonTestServer peer = new ProtonTestServer(serverOptions())) {
            peer.expectSaslXOauth2Connect(username, password);
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = connectionOptions();
            clientOptions.user(username);
            clientOptions.password(password);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), clientOptions);

            connection.openFuture().get(10, TimeUnit.SECONDS);
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSaslAnonymousConnection() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(serverOptions())) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);
            connection.close();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSaslFailureCodes() throws Exception {
        doSaslFailureCodesTestImpl(SASL_FAIL_AUTH);
        doSaslFailureCodesTestImpl(SASL_SYS);
        doSaslFailureCodesTestImpl(SASL_SYS_PERM);
        doSaslFailureCodesTestImpl(SASL_SYS_TEMP);
    }

    private void doSaslFailureCodesTestImpl(UnsignedByte saslFailureCode) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(serverOptions())) {
            peer.expectFailingSASLPlainConnect(saslFailureCode.byteValue(), "PLAIN", "ANONYMOUS");
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = connectionOptions();
            clientOptions.user("username");
            clientOptions.password("password");

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), clientOptions);

            try {
                connection.openFuture().get(10, TimeUnit.SECONDS);
            } catch (ExecutionException exe) {
                assertTrue(exe.getCause() instanceof ClientConnectionSecuritySaslException);
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Add a small delay after the SASL process fails, test peer will throw if
     * any unexpected frames arrive, such as erroneous open+close.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testWaitForUnexpectedFramesAfterSaslFailure() throws Exception {
        doMechanismSelectedTestImpl(null, null, ANONYMOUS, new String[] {ANONYMOUS}, true);
    }

    @Test
    public void testAnonymousSelectedWhenNoPasswordWasSupplied() throws Exception {
        doMechanismSelectedTestImpl("username", null, ANONYMOUS, new String[] {CRAM_MD5, PLAIN, ANONYMOUS}, false);
    }

    @Test
    public void testCramMd5SelectedWhenCredentialsPresent() throws Exception {
        doMechanismSelectedTestImpl("username", "password", CRAM_MD5, new String[] {CRAM_MD5, PLAIN, ANONYMOUS}, false);
    }

    @Test
    public void testScramSha1SelectedWhenCredentialsPresent() throws Exception {
        doMechanismSelectedTestImpl("username", "password", SCRAM_SHA_1, new String[] {SCRAM_SHA_1, CRAM_MD5, PLAIN, ANONYMOUS}, false);
    }

    @Test
    public void testScramSha256SelectedWhenCredentialsPresent() throws Exception {
        doMechanismSelectedTestImpl("username", "password", SCRAM_SHA_256, new String[] {SCRAM_SHA_256, SCRAM_SHA_1, CRAM_MD5, PLAIN, ANONYMOUS}, false);
    }

    @Test
    public void testScramSha512SelectedWhenCredentialsPresent() throws Exception {
        doMechanismSelectedTestImpl("username", "password", SCRAM_SHA_512, new String[] {SCRAM_SHA_512, SCRAM_SHA_256, SCRAM_SHA_1, CRAM_MD5, PLAIN, ANONYMOUS}, false);
    }

    @Test
    public void testXoauth2SelectedWhenCredentialsPresent() throws Exception {
        String token = Base64.getEncoder().encodeToString("token".getBytes(StandardCharsets.US_ASCII));
        doMechanismSelectedTestImpl("username", token, XOAUTH2, new String[] {XOAUTH2, ANONYMOUS}, false);
    }

    private void doMechanismSelectedTestImpl(String username, String password, String clientSelectedMech, String[] serverMechs, boolean wait) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(serverOptions())) {
            peer.expectSaslConnectThatAlwaysFailsAuthentication(serverMechs, clientSelectedMech);
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = connectionOptions();
            clientOptions.user(username);
            clientOptions.password(password);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), clientOptions);

            try {
                connection.openFuture().get(10, TimeUnit.SECONDS);
            } catch (ExecutionException exe) {
                assertTrue(exe.getCause() instanceof ClientConnectionSecuritySaslException);
            }

            if (wait) {
                Thread.sleep(200);
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testExternalSelectedWhenLocalPrincipalPresent() throws Exception {
        doMechanismSelectedExternalTestImpl(true, EXTERNAL, new String[] {EXTERNAL, SCRAM_SHA_512, SCRAM_SHA_256, SCRAM_SHA_1, CRAM_MD5, PLAIN, ANONYMOUS});
    }

    @Test
    public void testExternalNotSelectedWhenLocalPrincipalMissing() throws Exception {
        doMechanismSelectedExternalTestImpl(false, ANONYMOUS, new String[] {EXTERNAL, SCRAM_SHA_512, SCRAM_SHA_256, SCRAM_SHA_1, CRAM_MD5, PLAIN, ANONYMOUS});
    }

    private void doMechanismSelectedExternalTestImpl(boolean requireClientCert, String clientSelectedMech, String[] serverMechs) throws Exception {
        ProtonTestServerOptions serverOptions = serverOptions();
        serverOptions.setKeyStoreLocation(BROKER_JKS_KEYSTORE);
        serverOptions.setKeyStorePassword(PASSWORD);
        serverOptions.setVerifyHost(false);
        serverOptions.setSecure(true);
        if (requireClientCert) {
            serverOptions.setTrustStoreLocation(BROKER_JKS_TRUSTSTORE);
            serverOptions.setTrustStorePassword(PASSWORD);
            serverOptions.setNeedClientAuth(requireClientCert);
        }

        try (ProtonTestServer peer = new ProtonTestServer(serverOptions)) {
            peer.expectSaslConnectThatAlwaysFailsAuthentication(serverMechs, clientSelectedMech);
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = connectionOptions();
            clientOptions.sslOptions()
                         .sslEnabled(true)
                         .trustStoreLocation(CLIENT_JKS_TRUSTSTORE)
                         .trustStorePassword(PASSWORD);
            if (requireClientCert) {
                clientOptions.sslOptions().keyStoreLocation(CLIENT_JKS_KEYSTORE)
                                          .keyStorePassword(PASSWORD);
            }

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), clientOptions);

            try {
                connection.openFuture().get(10, TimeUnit.SECONDS);
            } catch (ExecutionException exe) {
                assertTrue(exe.getCause() instanceof ClientConnectionSecuritySaslException);
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testRestrictSaslMechanismsWithSingleMech() throws Exception {
        // Check PLAIN gets picked when we don't specify a restriction
        doMechanismSelectionRestrictedTestImpl("username", "password", PLAIN, new String[] { PLAIN, ANONYMOUS}, (String) null);

        // Check ANONYMOUS gets picked when we do specify a restriction
        doMechanismSelectionRestrictedTestImpl("username", "password", ANONYMOUS, new String[] { PLAIN, ANONYMOUS}, ANONYMOUS);
    }

    @Test
    public void testRestrictSaslMechanismsWithMultipleMechs() throws Exception {
        // Check CRAM-MD5 gets picked when we dont specify a restriction
        doMechanismSelectionRestrictedTestImpl("username", "password", CRAM_MD5, new String[] {CRAM_MD5, PLAIN, ANONYMOUS}, (String) null);

        // Check PLAIN gets picked when we specify a restriction with multiple mechs
        doMechanismSelectionRestrictedTestImpl("username", "password", PLAIN, new String[] { CRAM_MD5, PLAIN, ANONYMOUS}, "PLAIN", "ANONYMOUS");
    }

    @Test
    public void testRestrictSaslMechanismsWithMultipleMechsNoPassword() throws Exception {
        // Check ANONYMOUS gets picked when we specify a restriction with multiple mechs but don't give a password
        doMechanismSelectionRestrictedTestImpl("username", null, ANONYMOUS, new String[] { CRAM_MD5, PLAIN, ANONYMOUS}, "PLAIN", "ANONYMOUS");
    }

    private void doMechanismSelectionRestrictedTestImpl(String username, String password, String clientSelectedMech,
                                                        String[] serverMechs, String... allowedClientMechanisms) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer(serverOptions())) {
            peer.expectSaslConnectThatAlwaysFailsAuthentication(serverMechs, clientSelectedMech);
            peer.start();

            URI remoteURI = peer.getServerURI();

            ConnectionOptions clientOptions = connectionOptions();
            clientOptions.user(username);
            clientOptions.password(password);
            for (String mechanism : allowedClientMechanisms) {
                if (mechanism != null && !mechanism.isEmpty()) {
                    clientOptions.saslOptions().addAllowedMechanism(mechanism);
                }
            }

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), clientOptions);

            try {
                connection.openFuture().get(10, TimeUnit.SECONDS);
            } catch (ExecutionException exe) {
                assertTrue(exe.getCause() instanceof ClientConnectionSecuritySaslException);
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testMechanismNegotiationFailsToFindMatch() throws Exception {
        String[] serverMechs = new String[] { SCRAM_SHA_1, "UNKNOWN", PLAIN};

        String breadCrumb = "Could not find a suitable SASL Mechanism. " +
                            "No supported mechanism, or none usable with the available credentials.";

        try (ProtonTestServer peer = new ProtonTestServer(serverOptions())) {
            peer.expectSaslMechanismNegotiationFailure(serverMechs);
            peer.start();

            URI remoteURI = peer.getServerURI();

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            try {
                connection.openFuture().get(10, TimeUnit.SECONDS);
            } catch (ExecutionException exe) {
                assertTrue(exe.getCause() instanceof ClientConnectionSecuritySaslException);
                assertTrue(exe.getCause().getMessage().contains(breadCrumb));
            }

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
