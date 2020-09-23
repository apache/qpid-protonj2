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

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.DeliveryMode;
import org.apache.qpid.protonj2.client.OutputStreamOptions;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.StreamSender;
import org.apache.qpid.protonj2.client.StreamSenderMessage;
import org.apache.qpid.protonj2.client.StreamSenderOptions;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.HeaderMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transport.TransferPayloadCompositeMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedDataMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedPartialDataSectionMatcher;
import org.apache.qpid.protonj2.test.driver.netty.NettyTestPeer;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the {@link SendContext} implementation
 */
@Disabled
@Timeout(20)
public class StreamSenderTest extends ImperativeClientTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(StreamSenderTest.class);

    @Test
    public void testSendCustomMessageWithMultipleAmqpValueSections() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectBegin().respond(); // Hidden session for stream sender
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withLinkCredit(10).queue();
            peer.expectAttach().respond();  // Open a receiver to ensure sender link has processed
            peer.expectFlow();              // the inbound flow frame we sent previously before send.
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort()).openFuture().get();
            Session session = connection.openSession().openFuture().get();

            StreamSenderOptions options = new StreamSenderOptions();
            options.deliveryMode(DeliveryMode.AT_MOST_ONCE);
            options.writeBufferSize(Integer.MAX_VALUE);

            StreamSender sender = connection.openStreamSender("test-qos", options);

            // Create a custom message format send context and ensure that no early buffer writes take place
            StreamSenderMessage tracker = sender.beginMessage();

            tracker.messageFormat(17);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            HeaderMatcher headerMatcher = new HeaderMatcher(true);
            headerMatcher.withDurable(true);
            headerMatcher.withPriority((byte) 1);
            headerMatcher.withTtl(65535);
            headerMatcher.withFirstAcquirer(true);
            headerMatcher.withDeliveryCount(2);
            // Note: This is a specification violation but could be used by other message formats
            //       and we don't attempt to enforce at the Send Context what users write
            EncodedAmqpValueMatcher bodyMatcher1 = new EncodedAmqpValueMatcher("one", true);
            EncodedAmqpValueMatcher bodyMatcher2 = new EncodedAmqpValueMatcher("two", true);
            EncodedAmqpValueMatcher bodyMatcher3 = new EncodedAmqpValueMatcher("three", false);
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setHeadersMatcher(headerMatcher);
            payloadMatcher.addMessageContentMatcher(bodyMatcher1);
            payloadMatcher.addMessageContentMatcher(bodyMatcher2);
            payloadMatcher.addMessageContentMatcher(bodyMatcher3);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withMore(false).withMessageFormat(17).withPayload(payloadMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            // Populate all Header values
            Header header = new Header();
            header.setDurable(true);
            header.setPriority((byte) 1);
            header.setTimeToLive(65535);
            header.setFirstAcquirer(true);
            header.setDeliveryCount(2);

            tracker.write(header);
            tracker.write(new AmqpValue<>("one"));
            tracker.write(new AmqpValue<>("two"));
            tracker.write(new AmqpValue<>("three"));

            tracker.complete();

            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.close().get(10, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

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
            StreamSender sender = connection.openStreamSender("test-qos");
            StreamSenderMessage tracker = sender.beginMessage();

            OutputStreamOptions options = new OutputStreamOptions();
            OutputStream stream = tracker.dataOutputStream(options);

            assertNotNull(stream);

            sender.openFuture().get();

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
            StreamSender sender = connection.openStreamSender("test-queue");
            StreamSenderMessage sendContext = sender.beginMessage();

            // Populate all Header values
            Header header = new Header();
            header.setDurable(true);
            header.setPriority((byte) 1);
            header.setTimeToLive(65535);
            header.setFirstAcquirer(true);
            header.setDeliveryCount(2);

            sendContext.write(header);

            OutputStreamOptions options = new OutputStreamOptions();
            OutputStream stream = sendContext.dataOutputStream(options);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            // Stream won't output until some body bytes are written.
            stream.flush();
            stream.close();

            sender.close().get();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testFlushAfterFirstWriteEncodesAMQPHeaderSendContextBuffer() throws Exception {
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
            StreamSender sender = connection.openStreamSender("test-queue");
            StreamSenderMessage sendContext = sender.beginMessage();

            // Populate all Header values
            Header header = new Header();
            header.setDurable(true);
            header.setPriority((byte) 1);
            header.setTimeToLive(65535);
            header.setFirstAcquirer(true);
            header.setDeliveryCount(2);

            sendContext.write(header);

            OutputStreamOptions options = new OutputStreamOptions();
            OutputStream stream = sendContext.dataOutputStream(options);

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
            peer.expectTransfer().withPayload(payloadMatcher);
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
    void testAutoFlushAfterWritesExceedConfiguredBufferLimit() throws Exception {
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
            StreamSender sender = connection.openStreamSender("test-queue", new StreamSenderOptions().writeBufferSize(512));
            StreamSenderMessage tracker = sender.beginMessage();

            final byte[] payload = new byte[512];
            Arrays.fill(payload, (byte) 16);

            // Populate all Header values
            Header header = new Header();
            header.setDurable(true);
            header.setPriority((byte) 1);
            header.setTimeToLive(65535);
            header.setFirstAcquirer(true);
            header.setDeliveryCount(2);

            tracker.write(header);

            OutputStreamOptions options = new OutputStreamOptions();
            OutputStream stream = tracker.dataOutputStream(options);

            HeaderMatcher headerMatcher = new HeaderMatcher(true);
            headerMatcher.withDurable(true);
            headerMatcher.withPriority((byte) 1);
            headerMatcher.withTtl(65535);
            headerMatcher.withFirstAcquirer(true);
            headerMatcher.withDeliveryCount(2);
            EncodedDataMatcher dataMatcher = new EncodedDataMatcher(payload);
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setHeadersMatcher(headerMatcher);
            payloadMatcher.setMessageContentMatcher(dataMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher).withMore(true);

            // Stream won't output until some body bytes are written.
            stream.write(payload);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withNullPayload().withMore(false).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            stream.close();

            sender.close().get();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testAutoFlushDuringWriteThatExceedConfiguredBufferLimit() throws Exception {
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
            StreamSender sender = connection.openStreamSender("test-queue", new StreamSenderOptions().writeBufferSize(256));
            StreamSenderMessage tracker = sender.beginMessage();

            final byte[] payload = new byte[1024];
            Arrays.fill(payload, 0, 256, (byte) 1);
            Arrays.fill(payload, 256, 512, (byte) 2);
            Arrays.fill(payload, 512, 768, (byte) 3);
            Arrays.fill(payload, 768, 1024, (byte) 4);

            final byte[] payload1 = new byte[256];
            Arrays.fill(payload1, (byte) 1);
            final byte[] payload2 = new byte[256];
            Arrays.fill(payload2, (byte) 2);
            final byte[] payload3 = new byte[256];
            Arrays.fill(payload3, (byte) 3);
            final byte[] payload4 = new byte[256];
            Arrays.fill(payload4, (byte) 4);

            // Populate all Header values
            Header header = new Header();
            header.setDurable(true);
            header.setPriority((byte) 1);
            header.setTimeToLive(65535);
            header.setFirstAcquirer(true);
            header.setDeliveryCount(2);

            tracker.write(header);

            OutputStreamOptions options = new OutputStreamOptions();
            OutputStream stream = tracker.dataOutputStream(options);

            HeaderMatcher headerMatcher = new HeaderMatcher(true);
            headerMatcher.withDurable(true);
            headerMatcher.withPriority((byte) 1);
            headerMatcher.withTtl(65535);
            headerMatcher.withFirstAcquirer(true);
            headerMatcher.withDeliveryCount(2);
            EncodedDataMatcher dataMatcher1 = new EncodedDataMatcher(payload1);
            TransferPayloadCompositeMatcher payloadMatcher1 = new TransferPayloadCompositeMatcher();
            payloadMatcher1.setHeadersMatcher(headerMatcher);
            payloadMatcher1.setMessageContentMatcher(dataMatcher1);

            EncodedDataMatcher dataMatcher2 = new EncodedDataMatcher(payload2);
            TransferPayloadCompositeMatcher payloadMatcher2 = new TransferPayloadCompositeMatcher();
            payloadMatcher2.setMessageContentMatcher(dataMatcher2);

            EncodedDataMatcher dataMatcher3 = new EncodedDataMatcher(payload3);
            TransferPayloadCompositeMatcher payloadMatcher3 = new TransferPayloadCompositeMatcher();
            payloadMatcher3.setMessageContentMatcher(dataMatcher3);

            EncodedDataMatcher dataMatcher4 = new EncodedDataMatcher(payload4);
            TransferPayloadCompositeMatcher payloadMatcher4 = new TransferPayloadCompositeMatcher();
            payloadMatcher4.setMessageContentMatcher(dataMatcher4);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher1).withMore(true);
            peer.expectTransfer().withPayload(payloadMatcher2).withMore(true);
            peer.expectTransfer().withPayload(payloadMatcher3).withMore(true);
            peer.expectTransfer().withPayload(payloadMatcher4).withMore(true);

            // Stream won't output until some body bytes are written.
            stream.write(payload);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withNullPayload().withMore(false).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            stream.close();

            sender.close().get();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testCloseAfterSingleWriteEncodesAndCompletesTransferWhenNoStreamSizeConfigured() throws Exception {
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
            StreamSender sender = connection.openStreamSender("test-queue");
            StreamSenderMessage tracker = sender.beginMessage();

            // Populate all Header values
            Header header = new Header();
            header.setDurable(true);
            header.setPriority((byte) 1);
            header.setTimeToLive(65535);
            header.setFirstAcquirer(true);
            header.setDeliveryCount(2);

            tracker.write(header);

            OutputStreamOptions options = new OutputStreamOptions();
            OutputStream stream = tracker.dataOutputStream(options);

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
            peer.expectTransfer().withPayload(payloadMatcher).withMore(false).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            // Stream won't output until some body bytes are written.
            stream.write(new byte[] { 0, 1, 2, 3 });
            stream.close();

            sender.close().get();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testFlushAfterSecondWriteDoesNotEncodeAMQPHeaderFromConfiguration() throws Exception {
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
            StreamSender sender = connection.openStreamSender("test-queue");
            StreamSenderMessage tracker = sender.beginMessage();

            // Populate all Header values
            Header header = new Header();
            header.setDurable(true);
            header.setPriority((byte) 1);
            header.setTimeToLive(65535);
            header.setFirstAcquirer(true);
            header.setDeliveryCount(2);

            tracker.write(header);

            OutputStreamOptions options = new OutputStreamOptions();
            OutputStream stream = tracker.dataOutputStream(options);

            HeaderMatcher headerMatcher = new HeaderMatcher(true);
            headerMatcher.withDurable(true);
            headerMatcher.withPriority((byte) 1);
            headerMatcher.withTtl(65535);
            headerMatcher.withFirstAcquirer(true);
            headerMatcher.withDeliveryCount(2);
            EncodedDataMatcher dataMatcher1 = new EncodedDataMatcher(new byte[] { 0, 1, 2, 3 });
            TransferPayloadCompositeMatcher payloadMatcher1 = new TransferPayloadCompositeMatcher();
            payloadMatcher1.setHeadersMatcher(headerMatcher);
            payloadMatcher1.setMessageContentMatcher(dataMatcher1);

            // Second flush expectation
            EncodedDataMatcher dataMatcher2 = new EncodedDataMatcher(new byte[] { 4, 5, 6, 7 });
            TransferPayloadCompositeMatcher payloadMatcher2 = new TransferPayloadCompositeMatcher();
            payloadMatcher2.setMessageContentMatcher(dataMatcher2);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher1).withMore(true);
            peer.expectTransfer().withPayload(payloadMatcher2).withMore(true);
            peer.expectTransfer().withNullPayload().withMore(false).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            // Stream won't output until some body bytes are written.
            stream.write(new byte[] { 0, 1, 2, 3 });
            stream.flush();

            // Next write should only be a single Data section
            stream.write(new byte[] { 4, 5, 6, 7 });
            stream.flush();

            // Final Transfer that completes the Delivery
            stream.close();

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
            StreamSender sender = connection.openStreamSender("test-queue");
            StreamSenderMessage tracker = sender.beginMessage();

            final byte[] payload = new byte[] { 0, 1, 2, 3 };

            // Populate all Header values
            Header header = new Header();
            header.setDurable(true);
            header.setPriority((byte) 1);
            header.setDeliveryCount(1);

            tracker.write(header);

            OutputStreamOptions options = new OutputStreamOptions().streamSize(8192);
            OutputStream stream = tracker.dataOutputStream(options);

            HeaderMatcher headerMatcher = new HeaderMatcher(true);
            headerMatcher.withDurable(true);
            headerMatcher.withPriority((byte) 1);
            headerMatcher.withDeliveryCount(1);
            EncodedPartialDataSectionMatcher partialDataMatcher = new EncodedPartialDataSectionMatcher(8192, payload);
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setHeadersMatcher(headerMatcher);
            payloadMatcher.setMessageContentMatcher(partialDataMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher);
            peer.expectTransfer().withAborted(true).withNullPayload();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            stream.write(payload);
            stream.flush();

            // Stream should abort the send now since the configured size wasn't sent.
            stream.close();

            sender.close().get();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testIncompleteStreamClosureWithNoWritesDoesNotAbortTransfer() throws Exception {
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
            StreamSender sender = connection.openStreamSender("test-queue");
            StreamSenderMessage tracker = sender.beginMessage();

            // Populate all Header values
            Header header = new Header();
            header.setDurable(true);
            header.setPriority((byte) 1);
            header.setDeliveryCount(1);

            tracker.write(header);

            OutputStreamOptions options = new OutputStreamOptions().streamSize(8192).completeSendOnClose(false);
            OutputStream stream = tracker.dataOutputStream(options);

            HeaderMatcher headerMatcher = new HeaderMatcher(true);
            headerMatcher.withDurable(true);
            headerMatcher.withPriority((byte) 1);
            headerMatcher.withDeliveryCount(1);
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setHeadersMatcher(headerMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withAborted(anyOf(nullValue(), is(false))).withPayload(payloadMatcher).withMore(false);
            peer.expectDetach().respond();
            peer.expectClose().respond();

            stream.close();

            // This should finalize the Transfer since we asked the stream not to complete
            // and we didn't write anything before closing.
            tracker.complete();

            sender.close().get();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testCompleteStreamClosureCausesTransferCompleted() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withLinkCredit(3).queue();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            StreamSender sender = connection.openStreamSender("test-queue");
            StreamSenderMessage tracker = sender.beginMessage();

            final byte[] payload1 = new byte[] { 0, 1, 2, 3, 4, 5 };
            final byte[] payload2 = new byte[] { 6, 7, 8, 9, 10, 11, 12, 13, 14 };
            final byte[] payload3 = new byte[] { 15 };

            final int payloadSize = payload1.length + payload2.length + payload3.length;

            // Populate all Header values
            Header header = new Header();
            header.setDurable(true);
            header.setPriority((byte) 1);
            header.setDeliveryCount(1);

            tracker.write(header);

            OutputStreamOptions options = new OutputStreamOptions().streamSize(payloadSize);
            OutputStream stream = tracker.dataOutputStream(options);

            HeaderMatcher headerMatcher = new HeaderMatcher(true);
            headerMatcher.withDurable(true);
            headerMatcher.withPriority((byte) 1);
            headerMatcher.withDeliveryCount(1);
            EncodedPartialDataSectionMatcher partialDataMatcher = new EncodedPartialDataSectionMatcher(payloadSize, payload1);
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setHeadersMatcher(headerMatcher);
            payloadMatcher.setMessageContentMatcher(partialDataMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher);

            stream.write(payload1);
            stream.flush();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            partialDataMatcher = new EncodedPartialDataSectionMatcher(payload2);
            payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setMessageContentMatcher(partialDataMatcher);
            peer.expectTransfer().withMore(true).withPayload(partialDataMatcher);

            stream.write(payload2);
            stream.flush();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            partialDataMatcher = new EncodedPartialDataSectionMatcher(payload3);
            payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setMessageContentMatcher(partialDataMatcher);
            peer.expectTransfer().withMore(false).withPayload(partialDataMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            stream.write(payload3);
            stream.flush();

            // Stream should already be completed so no additional frames should be written.
            stream.close();

            sender.close().get();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
