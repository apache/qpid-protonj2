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
package org.apache.qpid.protonj2.test.driver;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.test.driver.codec.security.SaslCode;
import org.apache.qpid.protonj2.test.driver.codec.transport.AMQPHeader;
import org.apache.qpid.protonj2.test.driver.codec.transport.Open;
import org.apache.qpid.protonj2.test.driver.utils.TestPeerTestsBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the basics of the Proton Test Client implementation
 */
@Timeout(20)
class ProtonTestClientTest extends TestPeerTestsBase {

    private static final Logger LOG = LoggerFactory.getLogger(ProtonTestClientTest.class);

    @Test
    public void testClientCanConnectAndExchangeAMQPHeaders() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ProtonTestClient client = new ProtonTestClient();

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Thread.sleep(100);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testTwoClientConnectionsAndExchangeAMQPHeaders() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.start();

            URI remoteURI = peer.getServerURI();

            // Server can accept two connection, although not at the same time.

            try (ProtonTestClient client = new ProtonTestClient()) {
                client.connect(remoteURI.getHost(), remoteURI.getPort());
                client.expectAMQPHeader();
                client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
                client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            }

            try (ProtonTestClient client = new ProtonTestClient()) {
                client.connect(remoteURI.getHost(), remoteURI.getPort());
                client.expectAMQPHeader();
                client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
                client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            }

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testTwoClientConnectionsHandlesOpenBeginAttach() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().respond();
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            // Server can accept two connection, although not at the same time.

            try (ProtonTestClient client = new ProtonTestClient()) {
                client.connect(remoteURI.getHost(), remoteURI.getPort());
                client.expectAMQPHeader();
                client.expectOpen();
                client.expectBegin();
                client.expectAttach();
                client.dropAfterLastHandler(10);
                client.remoteAMQPHeader().now();
                client.remoteOpen().now();
                client.remoteBegin().now();
                client.remoteAttach().ofSender().now();
                client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            }

            try (ProtonTestClient client = new ProtonTestClient()) {
                client.connect(remoteURI.getHost(), remoteURI.getPort());
                client.expectAMQPHeader();
                client.expectOpen();
                client.expectBegin();
                client.expectAttach();
                client.remoteAMQPHeader().now();
                client.remoteOpen().now();
                client.remoteBegin().now();
                client.remoteAttach().ofSender().now();
                client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            }

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
    @Test
    public void testClientDetectsUnexpectedPerformativeResponseToAMQPHeader() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectAMQPHeader();
            peer.remoteOpen().queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ProtonTestClient client = new ProtonTestClient();

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();

            assertThrows(AssertionError.class, () -> client.waitForScriptToComplete(5, TimeUnit.SECONDS));

            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testClientDetectsUnexpectedPerformativeAndFailsTest() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen();
            peer.remoteBegin().queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ProtonTestClient client = new ProtonTestClient();

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();

            assertThrows(AssertionError.class, () -> client.waitForScriptToComplete(5, TimeUnit.SECONDS));

            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testClientCanConnectAndOpenExchanged() throws Exception {
        final AtomicReference<Open> capturedOpen = new AtomicReference<>();

        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().withCapture((o) -> capturedOpen.set(o)).respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ProtonTestClient client = new ProtonTestClient();

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectClose();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteClose().now();
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            assertNotNull(capturedOpen.get());
        }
    }

    @Test
    public void testClientCanConnectAndOpenExchangedAndFailWithUserPredicate() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ProtonTestClient client = new ProtonTestClient();

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectClose().withPredicate((o) -> false);
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteClose().later(10);

            assertThrows(AssertionError.class, () -> client.waitForScriptToComplete(5, TimeUnit.SECONDS));

            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testOpenInKindResponseCarriesDesiredCapabilities() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respondInKind();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ProtonTestClient client = new ProtonTestClient();

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen().withOfferedCapability("test");
            client.expectClose();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().withDesiredCapabilities("test").now();
            client.remoteClose().now();
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testClientFailsTestIfFrameSizeExpectationNotMet() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            ProtonTestClient client = new ProtonTestClient();

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen().withFrameSize(4096);
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();

            assertThrows(AssertionError.class, () -> client.waitForScriptToComplete(5, TimeUnit.SECONDS));

            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testClientPeerCanBeScriptedToConnectAndThenInitiateSASLAnonymousAuthentication() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect("PLAIN", "ANONYMOUS");
            peer.start();

            URI remoteURI = peer.getServerURI();

            final ProtonTestClient client = new ProtonTestClient();

            client.remoteSASLHeader().queue();
            client.expectSASLHeader();
            client.expectSaslMechanisms().withSaslServerMechanism("ANONYMOUS");
            client.remoteSaslInit().withMechanism("ANONYMOUS").queue();
            client.expectSaslOutcome().withCode(SaslCode.OK);
            client.remoteAMQPHeader().queue();
            client.connect(remoteURI.getHost(), remoteURI.getPort());

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testScriptedClientSASLAnonymousBeforeConnect() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect("PLAIN", "ANONYMOUS");
            peer.start();

            URI remoteURI = peer.getServerURI();

            ProtonTestClient client = new ProtonTestClient();

            client.queueClientSaslAnonymousConnect();
            client.connect(remoteURI.getHost(), remoteURI.getPort());

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testScriptedClientSASLAnonymousAfterConnect() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect("PLAIN", "ANONYMOUS");
            peer.start();

            URI remoteURI = peer.getServerURI();

            ProtonTestClient client = new ProtonTestClient();

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.triggerClientSaslAnonymousConnect();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testScriptedClientSASLPlainBeforeConnect() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLPlainConnect("test", "test");
            peer.start();

            URI remoteURI = peer.getServerURI();

            ProtonTestClient client = new ProtonTestClient();

            client.queueClientSaslPlainConnect("test", "test");
            client.connect(remoteURI.getHost(), remoteURI.getPort());

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testScriptedClientSASLPlainAfterConnect() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLPlainConnect("test", "test");
            peer.start();

            URI remoteURI = peer.getServerURI();

            ProtonTestClient client = new ProtonTestClient();

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.triggerClientSaslPlainConnect("test", "test");

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
