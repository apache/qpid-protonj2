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

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.MessageOutputStream;
import org.apache.qpid.protonj2.client.MessageOutputStreamOptions;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.HeaderMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transport.TransferPayloadCompositeMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedDataMatcher;
import org.apache.qpid.protonj2.test.driver.netty.NettyTestPeer;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the {@link MessageOutputStream} implementation
 */
@Timeout(20)
class MessageOutputStreamTest {

    private static final Logger LOG = LoggerFactory.getLogger(MessageOutputStreamTest.class);

    @Test
    void testCreateStream() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.expectDetach().withClosed(true).respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue").openFuture().get();

            MessageOutputStreamOptions options = new MessageOutputStreamOptions();
            MessageOutputStream stream = sender.outputStream(options);

            assertNotNull(stream);

            // Nothing should be sent since we closed without ever writing anything.
            stream.close();

            sender.close().get();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testFlushWithConfiguredNonBodySections() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withLinkCredit(1).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue").openFuture().get();

            // Populate all Header values
            Header header = new Header();
            header.setDurable(true);
            header.setPriority((byte) 1);
            header.setTimeToLive(65535);
            header.setFirstAcquirer(true);
            header.setDeliveryCount(2);

            MessageOutputStreamOptions options = new MessageOutputStreamOptions().header(header);
            MessageOutputStream stream = sender.outputStream(options);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            // Stream won't output until some body bytes are written.
            stream.flush();

            sender.close().get();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testFlushAfterFirstWriteEncodesAMQPHeaderFromConfiguration() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withLinkCredit(1).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue").openFuture().get();

            // Populate all Header values
            Header header = new Header();
            header.setDurable(true);
            header.setPriority((byte) 1);
            header.setTimeToLive(65535);
            header.setFirstAcquirer(true);
            header.setDeliveryCount(2);

            MessageOutputStreamOptions options = new MessageOutputStreamOptions().header(header);
            MessageOutputStream stream = sender.outputStream(options);

            HeaderMatcher headerMatcher = new HeaderMatcher(true);
            headerMatcher.withDurable(true);
            headerMatcher.withPriority((byte) 1);
            headerMatcher.withTtl(65535);
            headerMatcher.withFirstAcquirer(true);
            headerMatcher.withDeliveryCount(2);
            EncodedDataMatcher dataMatcher = new EncodedDataMatcher(new byte[] { 0, 1, 2, 3 });
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setHeadersMatcher(headerMatcher);
            payloadMatcher.setMessageContentMatcher(dataMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            // Stream won't output until some body bytes are written.
            stream.write(new byte[] { 0, 1, 2, 3 });
            stream.flush();

            sender.close().get();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testIncompleteStreamClosureCausesTransferAbort() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withLinkCredit(1).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();
            Sender sender = session.openSender("test-queue").openFuture().get();

            // Populate all Header values
            Header header = new Header();
            header.setDurable(true);
            header.setPriority((byte) 1);
            header.setDeliveryCount(1);

            MessageOutputStreamOptions options = new MessageOutputStreamOptions().header(header).outputLimit(8192);
            MessageOutputStream stream = sender.outputStream(options);

            HeaderMatcher headerMatcher = new HeaderMatcher(true);
            headerMatcher.withDurable(true);
            headerMatcher.withPriority((byte) 1);
            headerMatcher.withDeliveryCount(1);
            EncodedDataMatcher dataMatcher = new EncodedDataMatcher(new byte[] { 0, 1, 2, 3 });
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setHeadersMatcher(headerMatcher);
            payloadMatcher.setMessageContentMatcher(dataMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher);
            peer.expectTransfer().withAborted(true).withNullPayload();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            stream.write(new byte[] { 0, 1, 2, 3 });
            stream.flush();

            // Stream should abort the send now since the configured size wasn't sent.
            stream.close();

            sender.close().get();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
