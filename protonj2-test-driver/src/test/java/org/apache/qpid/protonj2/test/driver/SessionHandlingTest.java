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
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.test.driver.codec.transport.AMQPHeader;
import org.apache.qpid.protonj2.test.driver.utils.TestPeerTestsBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for the test driver session handling from both client and server perspectives.
 */
@Timeout(20)
class SessionHandlingTest extends TestPeerTestsBase {

    private static final Logger LOG = LoggerFactory.getLogger(SessionHandlingTest.class);

    @Test
    public void testSessionTrackingWithClientOpensSession() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().onChannel(0).respond();
            peer.expectEnd().onChannel(0).respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin().onChannel(0);
            client.expectEnd().onChannel(0);
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteEnd().now();
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSessionBeginResponseUsesScriptedChannel() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().onChannel(0).respond().onChannel(42);
            peer.expectEnd().onChannel(0).respond().onChannel(42);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin().withRemoteChannel(0).onChannel(42);
            client.expectEnd().onChannel(42);
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteEnd().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testWaitForCompletionFailsWhenRemoteSendEndOnWrongChannel() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().onChannel(0).respond().onChannel(42);
            peer.expectEnd().onChannel(0).respond().onChannel(43);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin().withRemoteChannel(0).onChannel(42);
            client.expectEnd().onChannel(42);
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteEnd().now();

            assertThrows(AssertionError.class, () -> client.waitForScriptToComplete(5, TimeUnit.SECONDS));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testServerEndResponseFillsChannelsAutomaticallyIfNoneSpecified() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().onChannel(0).respond().onChannel(42);
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin().withRemoteChannel(0).onChannel(42);
            client.expectEnd().onChannel(42);
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteEnd().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testServerRespondToLastBeginFeature() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().onChannel(0);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();

            // Wait for the above and then script next steps
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectBegin().withRemoteChannel(0).onChannel(42);

            // Now we respond to the last begin we saw at the server side.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectEnd().respond();
            peer.respondToLastBegin().onChannel(42).now();

            // Wait for the above and then script next steps
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectEnd().onChannel(42);
            client.remoteEnd().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testOpenAndCloseMultipleSessionsWithAutoChannelHandlingExpected() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().onChannel(0).respond();
            peer.expectBegin().onChannel(1).respond();
            peer.expectBegin().onChannel(2).respond();
            peer.expectEnd().onChannel(2).respond();
            peer.expectEnd().onChannel(1).respond();
            peer.expectEnd().onChannel(0).respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin().onChannel(0);
            client.expectBegin().onChannel(1);
            client.expectBegin().onChannel(2);
            client.connect(remoteURI.getHost(), remoteURI.getPort());

            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteBegin().now();
            client.remoteBegin().now();
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectEnd().onChannel(2);
            client.expectEnd().onChannel(1);
            client.expectEnd().onChannel(0);

            client.remoteEnd().onChannel(2).now();
            client.remoteEnd().onChannel(1).now();
            client.remoteEnd().onChannel(0).now();
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectClose();

            client.remoteClose().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testPeerEndsConnectionIfRemoteRespondsWithToHighChannelValue() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().withChannelMax(0).respond();
            peer.expectBegin();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().withChannelMax(0).now();
            client.remoteBegin().now();

            // Wait for the above and then script next steps
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectBegin();

            // Now we respond to the last begin we saw at the server side.
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.respondToLastBegin().onChannel(42).now();

            assertThrows(AssertionError.class, () -> client.waitForScriptToComplete(5, TimeUnit.SECONDS));
            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testPeerEnforcesChannelMaxOfZeroOnPipelinedOpenBegin() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen();
            peer.expectBegin();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().onChannel(42).now();

            // Wait for the above and then script next steps
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);

            assertThrows(AssertionError.class, () -> peer.waitForScriptToComplete(5, TimeUnit.SECONDS));
        }
    }
}
