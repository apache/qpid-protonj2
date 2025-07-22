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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.test.driver.codec.transport.AMQPHeader;
import org.apache.qpid.protonj2.test.driver.utils.TestPeerTestsBase;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for the test driver remote sender handling from both client and server perspectives.
 */
@Timeout(20)
class ReceiverHandlingTest extends TestPeerTestsBase {

    private static final Logger LOG = LoggerFactory.getLogger(ReceiverHandlingTest.class);

    @Test
    public void testReceiverTrackingWithClientOpensReceiver() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().onChannel(0).respond();
            peer.expectAttach().ofReceiver().withHandle(0).onChannel(0).respond();
            peer.expectEnd().onChannel(0).respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin().onChannel(0);
            client.expectAttach().ofSender().onChannel(0).withHandle(0);
            client.expectEnd().onChannel(0);
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofReceiver().now();
            client.remoteEnd().now();
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testAttachResponseUsesScriptedChannel() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond().withHandle(42);
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofReceiver().withHandle(42);
            client.expectEnd();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender().now();
            client.remoteEnd().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testWaitForCompletionFailsWhenRemoteSendDetachWithWrongHandle() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond().withHandle(42);
            peer.expectDetach().respond().withHandle(43);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofSender().withHandle(42);
            client.expectDetach().withHandle(42);
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofReceiver().now();
            client.remoteDetach().now();

            assertThrows(AssertionError.class, () -> client.waitForScriptToComplete(30, TimeUnit.SECONDS));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testServerDetachResponseFillsHandlesAutomaticallyIfNoneSpecified() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().respond().withHandle(42);
            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofSender().withHandle(42);
            client.expectDetach().withHandle(42);
            client.expectEnd();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofReceiver().now();
            client.remoteDetach().now();
            client.remoteEnd().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testServerRespondToLastAttachFeature() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofReceiver().now();

            // Wait for the above and then script next steps
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectAttach().ofSender();

            // Now we respond to the last begin we saw at the server side.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.respondToLastAttach().now();

            // Wait for the above and then script next steps
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectDetach();
            client.expectEnd();
            client.remoteDetach().now();
            client.remoteEnd().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testOpenAndCloseMultipleLinksWithAutoChannelHandlingExpected() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().withHandle(0).respond();
            peer.expectAttach().ofReceiver().withHandle(1).respond();
            peer.expectAttach().ofReceiver().withHandle(2).respond();
            peer.expectDetach().withHandle(2).respond();
            peer.expectDetach().withHandle(1).respond();
            peer.expectDetach().withHandle(0).respond();
            peer.expectEnd().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofSender().withHandle(0);
            client.expectAttach().ofSender().withHandle(1);
            client.expectAttach().ofSender().withHandle(2);
            client.connect(remoteURI.getHost(), remoteURI.getPort());

            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofReceiver().now();
            client.remoteAttach().ofReceiver().now();
            client.remoteAttach().ofReceiver().now();
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectDetach().withHandle(2);
            client.expectDetach().withHandle(1);
            client.expectDetach().withHandle(0);
            client.expectEnd();

            client.remoteDetach().withHandle(2).now();
            client.remoteDetach().withHandle(1).now();
            client.remoteDetach().withHandle(0).now();
            client.remoteEnd().now();
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectClose();

            client.remoteClose().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testPeerEndsConnectionIfRemoteRespondsWithToHighHandleValue() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().withHandleMax(0).respond();
            peer.expectAttach().ofReceiver();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().withHandleMax(0).now();
            client.remoteAttach().ofReceiver().now();

            // Wait for the above and then script next steps
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectAttach().ofSender();

            // Now we respond to the last attach we saw at the server side.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.respondToLastAttach().withHandle(42).now();

            assertThrows(AssertionError.class, () -> client.waitForScriptToComplete(5, TimeUnit.SECONDS));
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testPeerEnforcesHandleMaxOfZeroOnPipelinedOpenBeginAttach() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen();
            peer.expectBegin();
            peer.expectAttach().ofReceiver();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofReceiver().withHandle(42)
                                              .withSource().withCapabilities("QUEUE")
                                              .and().now();

            // Wait for the above and then script next steps
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);

            assertThrows(AssertionError.class, () -> peer.waitForScriptToComplete(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testNonInKindAttachResponseOffersCapabilitiesAreOmitted() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respondInKind();
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofReceiver().withOfferedCapability("test");
            client.expectEnd();
            client.remoteAMQPHeader().now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender().withDesiredCapabilities("test").now();
            client.remoteEnd().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSTackedOfferedAndDesiredCapabilityMatching() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respondInKind();
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofReceiver().withOfferedCapability("a") // Should fail unless stacking isn't done
                                              .withOfferedCapability("c");
            client.expectEnd();
            client.remoteAMQPHeader().now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender().withDesiredCapabilities("test", "c", "b")
                                            .now();
            client.remoteEnd().now();

            assertThrows(AssertionError.class, () -> client.waitForScriptToComplete(30, TimeUnit.SECONDS));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testInKindAttachResponseOffersCapabilitiesDesired() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofReceiver().withOfferedCapabilities(Matchers.nullValue());
            client.expectEnd();
            client.remoteAMQPHeader().now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender().withDesiredCapabilities("test").now();
            client.remoteEnd().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiverAttachWithJMSSelectorMatchingAPI() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().withSource().withJMSSelector("property=1").also().respond();
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofSender().withOfferedCapabilities(Matchers.nullValue());
            client.expectEnd();
            client.remoteAMQPHeader().now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofReceiver().withSource().withJMSSelector("property=1").and().now();
            client.remoteEnd().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiverAttachWithJMSSelectorMatchingAPIWithNonMatchingSelector() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().withSource().withJMSSelector("property=1").also().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.remoteAMQPHeader().now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofReceiver().withSource().withJMSSelector("property=2").and().now();

            client.waitForScriptToComplete();

            assertThrows(AssertionError.class, () -> peer.waitForScriptToComplete(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testReceiverAttachWithNoLocalMatchingAPI() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().withSource().withNoLocal().also().respond();
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofSender().withOfferedCapabilities(Matchers.nullValue());
            client.expectEnd();
            client.remoteAMQPHeader().now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofReceiver().withSource().withNoLocal().and().now();
            client.remoteEnd().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiverAttachWithNoLocalMatchingAPIButNoLocalNotSent() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().withSource().withNoLocal().also().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.remoteAMQPHeader().now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofReceiver().withSource().and().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);

            assertThrows(AssertionError.class, () -> peer.waitForScriptToComplete(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testReceiverAttachWithNoLocalAndJMSSelectorAPI() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().withSource().withNoLocal().withJMSSelector("property=1").also().respond();
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofSender().withOfferedCapabilities(Matchers.nullValue());
            client.expectEnd();
            client.remoteAMQPHeader().now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofReceiver().withSource().withNoLocal().withJMSSelector("property=1").and().now();
            client.remoteEnd().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiverAttachWithNoLocalAndJMSSelectorAPIButMissingNoLocal() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().withSource().withNoLocal().withJMSSelector("property=1").also().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.remoteAMQPHeader().now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofReceiver().withSource().withJMSSelector("property=1").and().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);

            assertThrows(AssertionError.class, () -> peer.waitForScriptToComplete(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testReceiverAttachWithNoLocalAndJMSSelectorAPIButMissingJMSSelector() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofReceiver().withSource().withNoLocal().withJMSSelector("property=1").also().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.remoteAMQPHeader().now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofReceiver().withSource().withNoLocal().and().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);

            assertThrows(AssertionError.class, () -> peer.waitForScriptToComplete(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testReceiverSendsRejectedDisposition() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respondInKind();
            peer.remoteFlow().withLinkCredit(1).queue();
            peer.expectTransfer().respond().withSettled(true).withState().rejected("error", "Error Code: 111222");
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofReceiver();
            client.expectFlow().withLinkCredit(1);
            client.remoteTransfer().withDeliveryId(0).withMessage().withBody().withValue("test").and().queue();
            client.expectDisposition().withSettled(true).withState().rejected("error", Matchers.containsString("111222"));
            client.remoteEnd().queue();
            client.expectEnd();

            // Initiate the exchange
            client.remoteAMQPHeader().now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiverSendsModifiedDisposition() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respondInKind();
            peer.remoteFlow().withLinkCredit(1).queue();
            peer.expectTransfer().respond().withSettled(true).withState().modified(true, true);
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofReceiver();
            client.expectFlow().withLinkCredit(1);
            client.remoteTransfer().withDeliveryId(0).withMessage().withBody().withValue("test").and().queue();
            client.expectDisposition().withSettled(true).withState().modified(true, true);
            client.remoteEnd().queue();
            client.expectEnd();

            // Initiate the exchange
            client.remoteAMQPHeader().now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReceiverSendsModifiedDispositionWithAnnotations() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            final Map<String, Object> annotations = Map.of("test", "value");

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respondInKind();
            peer.remoteFlow().withLinkCredit(1).queue();
            peer.expectTransfer().respond().withSettled(true).withState().modified(true, true, annotations);
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofReceiver();
            client.expectFlow().withLinkCredit(1);
            client.remoteTransfer().withDeliveryId(0).withMessage().withBody().withValue("test").and().queue();
            client.expectDisposition().withSettled(true).withState().modified(true, true, annotations);
            client.remoteEnd().queue();
            client.expectEnd();

            // Initiate the exchange
            client.remoteAMQPHeader().now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
