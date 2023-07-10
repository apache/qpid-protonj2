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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.test.driver.codec.transport.AMQPHeader;
import org.apache.qpid.protonj2.test.driver.utils.TestPeerTestsBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for the test driver remote sender handling from both client and server perspectives.
 */
@Timeout(20)
class SenderHandlingTest extends TestPeerTestsBase {

    private static final Logger LOG = LoggerFactory.getLogger(SenderHandlingTest.class);

    @Test
    public void testSenderTrackingWithClientOpensSender() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().onChannel(0).respond();
            peer.expectAttach().ofSender().withHandle(0).onChannel(0).respond();
            peer.expectEnd().onChannel(0).respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin().onChannel(0);
            client.expectAttach().ofReceiver().onChannel(0).withHandle(0);
            client.expectEnd().onChannel(0);
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender().now();
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
            peer.expectAttach().ofSender().respond().withHandle(42);
            peer.expectDetach().respond().withHandle(43);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofReceiver().withHandle(42);
            client.expectDetach().withHandle(42);
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender().now();
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
            peer.expectAttach().ofSender().respond().withHandle(42);
            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofReceiver().withHandle(42);
            client.expectDetach().withHandle(42);
            client.expectEnd();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender().now();
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
            peer.expectAttach().ofSender();
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
            client.remoteAttach().ofSender().now();

            // Wait for the above and then script next steps
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectAttach().ofReceiver();

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
            peer.expectAttach().ofSender().withHandle(0).respond();
            peer.expectAttach().ofSender().withHandle(1).respond();
            peer.expectAttach().ofSender().withHandle(2).respond();
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
            client.expectAttach().ofReceiver().withHandle(0);
            client.expectAttach().ofReceiver().withHandle(1);
            client.expectAttach().ofReceiver().withHandle(2);
            client.connect(remoteURI.getHost(), remoteURI.getPort());

            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender().now();
            client.remoteAttach().ofSender().now();
            client.remoteAttach().ofSender().now();
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
            peer.expectAttach().ofSender();
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
            client.remoteAttach().ofSender().now();

            // Wait for the above and then script next steps
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectAttach().ofReceiver();

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
            peer.expectAttach().ofSender();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender().withHandle(42).now();

            // Wait for the above and then script next steps
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);

            assertThrows(AssertionError.class, () -> peer.waitForScriptToComplete(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testTransferAutoPopulatesDeliveryTagAndHandleFromLastOpenedLink() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond().withHandle(42);
            peer.remoteFlow().withLinkCredit(1).queue();
            peer.expectTransfer().withHandle(2)
                                 .withDeliveryId(0)
                                 .withNonNullDeliveryTag()
                                 .withNonNullPayload();
            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofReceiver().withHandle(42);
            client.expectFlow().withLinkCredit(1).withHandle(42);
            client.remoteTransfer().withDeliveryId(0).withPayload(new byte[] {0}).queue();
            // Now start and then await the remote grant of credit and out send of a transfer
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender().withHandle(2).now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectDetach().withHandle(42);
            client.expectEnd();

            client.remoteDetach().now();
            client.remoteEnd().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateClientConnectionWithPipelinedOpen() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.start();

            URI remoteURI = peer.getServerURI();

            client.connect(remoteURI.getHost(), remoteURI.getPort());

            // These should not be sent until a non-deferred action is triggered.
            client.remoteAMQPHeader().deferred().now();
            client.remoteOpen().deferred().now();
            client.remoteBegin().deferred().now();
            client.remoteAttach().ofSender().deferred().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().onChannel(0).respond();
            peer.expectAttach().ofSender().withHandle(0).onChannel(0).respond();
            peer.expectEnd().onChannel(0).respond();

            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin().onChannel(0);
            client.expectAttach().ofReceiver().onChannel(0).withHandle(0);
            client.expectEnd().onChannel(0);
            client.remoteEnd().now(); // Trigger all deferred writes
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendRemoteCommandsWithSingularPropertyAPIs() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            final Map<String, Object> expecetedProperties = new HashMap<>();
            expecetedProperties.put("test", "entry");

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().withProperties(expecetedProperties);
            peer.expectBegin().withProperties(expecetedProperties);
            peer.expectAttach().ofSender().withProperties(expecetedProperties);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.remoteAMQPHeader().now();
            client.remoteOpen().withProperty("test", "entry").now();
            client.remoteBegin().withProperty("test", "entry").now();
            client.remoteAttach().ofSender().withProperty("test", "entry").now();

            // Wait for the above and then script next steps
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendRemoteCommandsWithSingularPropertyAPIsForBoth() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().withProperty("test1", "entry");
            peer.expectBegin().withProperty("test2", "entry");
            peer.expectAttach().ofSender().withProperty("test3", "entry");
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.remoteAMQPHeader().now();
            client.remoteOpen().withProperty("test1", "entry").now();
            client.remoteBegin().withProperty("test2", "entry").now();
            client.remoteAttach().ofSender().withProperty("test3", "entry").now();

            // Wait for the above and then script next steps
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendRemoteAttachExpectingSinglePropertyFails() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen();
            peer.expectBegin();
            peer.expectAttach().ofSender().withProperty("test", "entry");
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.remoteAMQPHeader().now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender().withProperty("fail", "entry").now();

            // Wait for the above and then script next steps
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);

            assertThrows(AssertionError.class, () -> peer.waitForScriptToComplete(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testSenderAttachContainsAtLeastOneMatchedProperty() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen();
            peer.expectBegin();
            peer.expectAttach().ofSender().withProperty("test", "entry");
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.remoteAMQPHeader().now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender().withProperty("test", "entry")
                                            .withProperty("another", "property").now();

            // Wait for the above and then script next steps
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDetachCanExpectMatcherInDescription() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen();
            peer.expectBegin();
            peer.expectAttach().ofSender();
            peer.expectDetach().withError("amqp:not-authorized", equalTo("test"));
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.remoteAMQPHeader().now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender().now();
            client.remoteDetach().withErrorCondition("amqp:not-authorized", "test").now();

            // Wait for the above and then script next steps
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
