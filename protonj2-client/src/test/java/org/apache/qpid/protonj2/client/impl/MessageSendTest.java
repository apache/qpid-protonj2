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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.AdvancedMessage;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.DeliveryMode;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.SenderOptions;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.client.util.ExternalMessage;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.ApplicationPropertiesMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.DeliveryAnnotationsMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.FooterMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.HeaderMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.MessageAnnotationsMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.PropertiesMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transport.TransferPayloadCompositeMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedAmqpSequenceMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedDataMatcher;
import org.apache.qpid.protonj2.types.messaging.AmqpSequence;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests expectations on sends using the various {@link Message} and {@link AdvancedMessage}
 * API mechanisms
 */
@Timeout(20)
class MessageSendTest extends ImperativeClientTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(MessageSendTest.class);

    @Test
    public void testSendMessageWithHeaderValuesPopulated() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
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
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withMessage().withMessageFormat(0)
                                               .withHeader()
                                               .withDurability(true)
                                               .withPriority((byte) 1)
                                               .withTimeToLive(65535)
                                               .withFirstAcquirer(true)
                                               .withDeliveryCount(2)
                                               .also()
                                               .withValue("Hello World");
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final Message<String> message = Message.create("Hello World");

            // Populate all Header values
            message.durable(true);
            message.priority((byte) 1);
            message.timeToLive(65535);
            message.firstAcquirer(true);
            message.deliveryCount(2);

            final Tracker tracker = sender.send(message);

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendMessageWithPropertiesValuesPopulated() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
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
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            PropertiesMatcher propertiesMatcher = new PropertiesMatcher(true);
            propertiesMatcher.withMessageId("ID:12345");
            propertiesMatcher.withUserId("user".getBytes(StandardCharsets.UTF_8));
            propertiesMatcher.withTo("the-management");
            propertiesMatcher.withSubject("amqp");
            propertiesMatcher.withReplyTo("the-minions");
            propertiesMatcher.withCorrelationId("abc");
            propertiesMatcher.withContentEncoding("application/json");
            propertiesMatcher.withContentEncoding("gzip");
            propertiesMatcher.withAbsoluteExpiryTime(123);
            propertiesMatcher.withCreationTime(1);
            propertiesMatcher.withGroupId("disgruntled");
            propertiesMatcher.withGroupSequence(8192);
            propertiesMatcher.withReplyToGroupId("/dev/null");
            EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher("Hello World");
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setPropertiesMatcher(propertiesMatcher);
            payloadMatcher.setMessageContentMatcher(bodyMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final Message<String> message = Message.create("Hello World");

            // Populate all Properties values
            message.messageId("ID:12345");
            message.userId("user".getBytes(StandardCharsets.UTF_8));
            message.to("the-management");
            message.subject("amqp");
            message.replyTo("the-minions");
            message.correlationId("abc");
            message.contentEncoding("application/json");
            message.contentEncoding("gzip");
            message.absoluteExpiryTime(123);
            message.creationTime(1);
            message.groupId("disgruntled");
            message.groupSequence(8192);
            message.replyToGroupId("/dev/null");

            final Tracker tracker = sender.send(message);

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendMessageWithDeliveryAnnotationsPopulated() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
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
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            DeliveryAnnotationsMatcher daMatcher = new DeliveryAnnotationsMatcher(true);
            daMatcher.withEntry("one", 1);
            daMatcher.withEntry("two", 2);
            daMatcher.withEntry("three", Matchers.equalTo(3));
            EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher("Hello World");
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setDeliveryAnnotationsMatcher(daMatcher);
            payloadMatcher.setMessageContentMatcher(bodyMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final Message<String> message = Message.create("Hello World");

            // Populate delivery annotations
            Map<String, Object> deliveryAnnotations = new HashMap<>();
            deliveryAnnotations.put("one", 1);
            deliveryAnnotations.put("two", 2);
            deliveryAnnotations.put("three", 3);

            final Tracker tracker = sender.send(message, deliveryAnnotations);

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendMessageWithMessageAnnotationsPopulated() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
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
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            MessageAnnotationsMatcher maMatcher = new MessageAnnotationsMatcher(true);
            maMatcher.withEntry("one", Matchers.equalTo(1));
            maMatcher.withEntry("two", 2);
            maMatcher.withEntry("three", Matchers.equalTo(3));
            EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher("Hello World");
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setMessageAnnotationsMatcher(maMatcher);
            payloadMatcher.setMessageContentMatcher(bodyMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final Message<String> message = Message.create("Hello World");

            // Populate message annotations
            message.annotation("one", 1);
            message.annotation("two", 2);
            message.annotation("three", 3);

            final Tracker tracker = sender.send(message);

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendMessageWithApplicationPropertiesPopulated() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
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
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            ApplicationPropertiesMatcher apMatcher = new ApplicationPropertiesMatcher(true);
            apMatcher.withEntry("one", 1);
            apMatcher.withEntry("two", 2);
            apMatcher.withEntry("three", 3);
            EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher("Hello World");
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setApplicationPropertiesMatcher(apMatcher);
            payloadMatcher.setMessageContentMatcher(bodyMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final Message<String> message = Message.create("Hello World");

            // Populate message application properties
            message.property("one", 1);
            message.property("two", 2);
            message.property("three", 3);

            final Tracker tracker = sender.send(message);

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendMessageWithFootersPopulated() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
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
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            FooterMatcher footerMatcher = new FooterMatcher(false);
            footerMatcher.withEntry("f1", Matchers.equalTo(1));
            footerMatcher.withEntry("f2", Matchers.equalTo(2));
            footerMatcher.withEntry("f3", Matchers.equalTo(3));
            EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher("Hello World", true);
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setMessageContentMatcher(bodyMatcher);
            payloadMatcher.setFootersMatcher(footerMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final Message<String> message = Message.create("Hello World");

            // Populate message footers
            message.footer("f1", 1);
            message.footer("f2", 2);
            message.footer("f3", 3);

            final Tracker tracker = sender.send(message);

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendMessageWithMultipleSectionsPopulated() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
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
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            HeaderMatcher headerMatcher = new HeaderMatcher(true);
            headerMatcher.withDurable(true);
            headerMatcher.withPriority((byte) 1);
            headerMatcher.withTtl(65535);
            headerMatcher.withFirstAcquirer(true);
            headerMatcher.withDeliveryCount(2);
            PropertiesMatcher propertiesMatcher = new PropertiesMatcher(true);
            propertiesMatcher.withMessageId("ID:12345");
            propertiesMatcher.withUserId("user".getBytes(StandardCharsets.UTF_8));
            propertiesMatcher.withTo("the-management");
            propertiesMatcher.withSubject("amqp");
            propertiesMatcher.withReplyTo("the-minions");
            propertiesMatcher.withCorrelationId("abc");
            propertiesMatcher.withContentEncoding("application/json");
            propertiesMatcher.withContentEncoding("gzip");
            propertiesMatcher.withAbsoluteExpiryTime(123);
            propertiesMatcher.withCreationTime(1);
            propertiesMatcher.withGroupId("disgruntled");
            propertiesMatcher.withGroupSequence(8192);
            propertiesMatcher.withReplyToGroupId("/dev/null");
            DeliveryAnnotationsMatcher daMatcher = new DeliveryAnnotationsMatcher(true);
            daMatcher.withEntry("da1", Matchers.equalTo(1));
            daMatcher.withEntry("da2", Matchers.equalTo(2));
            daMatcher.withEntry("da3", Matchers.equalTo(3));
            MessageAnnotationsMatcher maMatcher = new MessageAnnotationsMatcher(true);
            maMatcher.withEntry("ma1", Matchers.equalTo(1));
            maMatcher.withEntry("ma2", Matchers.equalTo(2));
            maMatcher.withEntry("ma3", Matchers.equalTo(3));
            ApplicationPropertiesMatcher apMatcher = new ApplicationPropertiesMatcher(true);
            apMatcher.withEntry("ap1", Matchers.equalTo(1));
            apMatcher.withEntry("ap2", Matchers.equalTo(2));
            apMatcher.withEntry("ap3", Matchers.equalTo(3));
            EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher("Hello World", true);
            FooterMatcher footerMatcher = new FooterMatcher(false);
            footerMatcher.withEntry("f1", Matchers.equalTo(1));
            footerMatcher.withEntry("f2", Matchers.equalTo(2));
            footerMatcher.withEntry("f3", Matchers.equalTo(3));
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setHeadersMatcher(headerMatcher);
            payloadMatcher.setDeliveryAnnotationsMatcher(daMatcher);
            payloadMatcher.setMessageAnnotationsMatcher(maMatcher);
            payloadMatcher.setPropertiesMatcher(propertiesMatcher);
            payloadMatcher.setApplicationPropertiesMatcher(apMatcher);
            payloadMatcher.setMessageContentMatcher(bodyMatcher);
            payloadMatcher.setFootersMatcher(footerMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            // Populate delivery annotations
            Map<String, Object> deliveryAnnotations = new HashMap<>();
            deliveryAnnotations.put("da1", 1);
            deliveryAnnotations.put("da2", 2);
            deliveryAnnotations.put("da3", 3);

            final Message<String> message = Message.create("Hello World");

            // Populate all Header values
            message.durable(true);
            message.priority((byte) 1);
            message.timeToLive(65535);
            message.firstAcquirer(true);
            message.deliveryCount(2);
            // Populate message annotations
            message.annotation("ma1", 1);
            message.annotation("ma2", 2);
            message.annotation("ma3", 3);
            // Populate all Properties values
            message.messageId("ID:12345");
            message.userId("user".getBytes(StandardCharsets.UTF_8));
            message.to("the-management");
            message.subject("amqp");
            message.replyTo("the-minions");
            message.correlationId("abc");
            message.contentEncoding("application/json");
            message.contentEncoding("gzip");
            message.absoluteExpiryTime(123);
            message.creationTime(1);
            message.groupId("disgruntled");
            message.groupSequence(8192);
            message.replyToGroupId("/dev/null");
            // Populate message application properties
            message.property("ap1", 1);
            message.property("ap2", 2);
            message.property("ap3", 3);
            // Populate message footers
            message.footer("f1", 1);
            message.footer("f2", 2);
            message.footer("f3", 3);

            final Tracker tracker = sender.send(message, deliveryAnnotations);

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendMessageWithUUIDPayloadArrivesWithAMQPValueBodySetFromEmpty() throws Exception {
        doTestSendMessageWithUUIDPayloadArrivesWithAMQPValueBody(true);
    }

    @Test
    public void testSendMessageWithUUIDPayloadArrivesWithAMQPValueBodyPopulateOnCreate() throws Exception {
        doTestSendMessageWithUUIDPayloadArrivesWithAMQPValueBody(false);
    }

    private void doTestSendMessageWithUUIDPayloadArrivesWithAMQPValueBody(boolean useSetter) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withLinkCredit(10).queue();
            peer.expectAttach().respond();  // Open a receiver to ensure sender link has processed
            peer.expectFlow();              // the inbound flow frame we sent previously before send.
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            final UUID payload = UUID.randomUUID();

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort()).openFuture().get();

            Session session = connection.openSession().openFuture().get();
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher(payload);
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setMessageContentMatcher(bodyMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final Message<UUID> message;
            if (useSetter) {
                message = Message.<UUID>create().body(payload);
            } else {
                message = Message.create(payload);
            }

            final Tracker tracker = sender.send(message);

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendMessageWithByteArrayPayloadArrivesWithDataSectionSetFromEmpty() throws Exception {
        doTestSendMessageWithByteArrayPayloadArrivesWithDataSection(true);
    }

    @Test
    public void testSendMessageWithByteArrayPayloadArrivesWithDataSectionPopulateOnCreate() throws Exception {
        doTestSendMessageWithByteArrayPayloadArrivesWithDataSection(false);
    }

    private void doTestSendMessageWithByteArrayPayloadArrivesWithDataSection(boolean useSetter) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withLinkCredit(10).queue();
            peer.expectAttach().respond();  // Open a receiver to ensure sender link has processed
            peer.expectFlow();              // the inbound flow frame we sent previously before send.
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            final byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort()).openFuture().get();

            Session session = connection.openSession().openFuture().get();
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            EncodedDataMatcher bodyMatcher = new EncodedDataMatcher(payload);
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setMessageContentMatcher(bodyMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final Message<byte[]> message;
            if (useSetter) {
                message = Message.<byte[]>create().body(payload);
            } else {
                message = Message.create(payload);
            }

            final Tracker tracker = sender.send(message);

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendMessageWithListPayloadArrivesWithAMQPSequenceBodySetFromEmpty() throws Exception {
        doTestSendMessageWithListPayloadArrivesWithAMQPSequenceBody(true);
    }

    @Test
    public void testSendMessageWithListPayloadArrivesWithAMQPSequenceBodyPopulateOnCreate() throws Exception {
        doTestSendMessageWithListPayloadArrivesWithAMQPSequenceBody(false);
    }

    private void doTestSendMessageWithListPayloadArrivesWithAMQPSequenceBody(boolean useSetter) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withLinkCredit(10).queue();
            peer.expectAttach().respond();  // Open a receiver to ensure sender link has processed
            peer.expectFlow();              // the inbound flow frame we sent previously before send.
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            final List<UUID> payload = new ArrayList<>();
            payload.add(UUID.randomUUID());
            payload.add(UUID.randomUUID());
            payload.add(UUID.randomUUID());

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort()).openFuture().get();

            Session session = connection.openSession().openFuture().get();
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            EncodedAmqpSequenceMatcher bodyMatcher = new EncodedAmqpSequenceMatcher(payload);
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setMessageContentMatcher(bodyMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final Message<List<UUID>> message;
            if (useSetter) {
                message = Message.<List<UUID>>create().body(payload);
            } else {
                message = Message.create(payload);
            }

            final Tracker tracker = sender.send(message);

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendMessageWithMapPayloadArrivesWithAMQPValueBodySetFromEmpty() throws Exception {
        doTestSendMessageWithMapPayloadArrivesWithAMQPValueBody(true);
    }

    @Test
    public void testSendMessageWithMapPayloadArrivesWithAMQPValueBodyPopulateOnCreate() throws Exception {
        doTestSendMessageWithMapPayloadArrivesWithAMQPValueBody(false);
    }

    private void doTestSendMessageWithMapPayloadArrivesWithAMQPValueBody(boolean useSetter) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().ofSender().respond();
            peer.remoteFlow().withLinkCredit(10).queue();
            peer.expectAttach().respond();  // Open a receiver to ensure sender link has processed
            peer.expectFlow();              // the inbound flow frame we sent previously before send.
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Sender test started, peer listening on: {}", remoteURI);

            final Map<String, UUID> payload = new HashMap<>();
            payload.put("1", UUID.randomUUID());
            payload.put("2", UUID.randomUUID());
            payload.put("3", UUID.randomUUID());

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort()).openFuture().get();

            Session session = connection.openSession().openFuture().get();
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher(payload);
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setMessageContentMatcher(bodyMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final Message<Map<String, UUID>> message;
            if (useSetter) {
                message = Message.<Map<String, UUID>>create().body(payload);
            } else {
                message = Message.create(payload);
            }

            final Tracker tracker = sender.send(message);

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConvertMessageToAdvancedAndSendAMQPHeader() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
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
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            HeaderMatcher headerMatcher = new HeaderMatcher(false);
            headerMatcher.withDurable(true);
            headerMatcher.withPriority((byte) 1);
            headerMatcher.withTtl(65535);
            headerMatcher.withFirstAcquirer(true);
            headerMatcher.withDeliveryCount(2);
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setHeadersMatcher(headerMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final Message<String> message = Message.create();
            final AdvancedMessage<String> advanced = message.toAdvancedMessage();

            // Populate all Header values
            Header header = new Header();
            header.setDurable(true);
            header.setPriority((byte) 1);
            header.setTimeToLive(65535);
            header.setFirstAcquirer(true);
            header.setDeliveryCount(2);

            advanced.header(header);

            final Tracker tracker = sender.send(advanced);

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendOfExternalMessageWithoutAdvancedConversionSupport() throws Exception {
        doTestSendOfExternalMessage(false, false);
    }

    @Test
    public void testSendOfExternalMessageWithAdvancedConversionSupport() throws Exception {
        doTestSendOfExternalMessage(true, false);
    }

    @Test
    public void testTrySendOfExternalMessageWithoutAdvancedConversionSupport() throws Exception {
        doTestSendOfExternalMessage(false, true);
    }

    @Test
    public void testTrySendOfExternalMessageWithAdvancedConversionSupport() throws Exception {
        doTestSendOfExternalMessage(true, true);
    }

    private void doTestSendOfExternalMessage(boolean allowAdvancedConversion, boolean trySend) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
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
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            HeaderMatcher headerMatcher = new HeaderMatcher(true);
            headerMatcher.withDurable(true);
            headerMatcher.withPriority((byte) 1);
            headerMatcher.withTtl(65535);
            headerMatcher.withFirstAcquirer(true);
            headerMatcher.withDeliveryCount(2);
            PropertiesMatcher propertiesMatcher = new PropertiesMatcher(true);
            propertiesMatcher.withMessageId("ID:12345");
            propertiesMatcher.withUserId("user".getBytes(StandardCharsets.UTF_8));
            propertiesMatcher.withTo("the-management");
            propertiesMatcher.withSubject("amqp");
            propertiesMatcher.withReplyTo("the-minions");
            propertiesMatcher.withCorrelationId("abc");
            propertiesMatcher.withContentEncoding("application/json");
            propertiesMatcher.withContentEncoding("gzip");
            propertiesMatcher.withAbsoluteExpiryTime(123);
            propertiesMatcher.withCreationTime(1);
            propertiesMatcher.withGroupId("disgruntled");
            propertiesMatcher.withGroupSequence(8192);
            propertiesMatcher.withReplyToGroupId("/dev/null");
            DeliveryAnnotationsMatcher daMatcher = new DeliveryAnnotationsMatcher(true);
            daMatcher.withEntry("da1", Matchers.equalTo(1));
            daMatcher.withEntry("da2", Matchers.equalTo(2));
            daMatcher.withEntry("da3", Matchers.equalTo(3));
            MessageAnnotationsMatcher maMatcher = new MessageAnnotationsMatcher(true);
            maMatcher.withEntry("ma1", Matchers.equalTo(1));
            maMatcher.withEntry("ma2", Matchers.equalTo(2));
            maMatcher.withEntry("ma3", Matchers.equalTo(3));
            ApplicationPropertiesMatcher apMatcher = new ApplicationPropertiesMatcher(true);
            apMatcher.withEntry("ap1", Matchers.equalTo(1));
            apMatcher.withEntry("ap2", Matchers.equalTo(2));
            apMatcher.withEntry("ap3", Matchers.equalTo(3));
            FooterMatcher footerMatcher = new FooterMatcher(false);
            footerMatcher.withEntry("f1", Matchers.equalTo(1));
            footerMatcher.withEntry("f2", Matchers.equalTo(2));
            footerMatcher.withEntry("f3", Matchers.equalTo(3));
            EncodedAmqpValueMatcher bodyMatcher = new EncodedAmqpValueMatcher("Hello World", true);
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.setHeadersMatcher(headerMatcher);
            payloadMatcher.setDeliveryAnnotationsMatcher(daMatcher);
            payloadMatcher.setMessageAnnotationsMatcher(maMatcher);
            payloadMatcher.setPropertiesMatcher(propertiesMatcher);
            payloadMatcher.setApplicationPropertiesMatcher(apMatcher);
            payloadMatcher.setMessageContentMatcher(bodyMatcher);
            payloadMatcher.setFootersMatcher(footerMatcher);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            // Populate delivery annotations
            Map<String, Object> deliveryAnnotations = new HashMap<>();
            deliveryAnnotations.put("da1", 1);
            deliveryAnnotations.put("da2", 2);
            deliveryAnnotations.put("da3", 3);

            final Message<String> message = new ExternalMessage<>(allowAdvancedConversion);

            message.body("Hello World");
            // Populate all Header values
            message.durable(true);
            message.priority((byte) 1);
            message.timeToLive(65535);
            message.firstAcquirer(true);
            message.deliveryCount(2);
            // Populate message annotations
            message.annotation("ma1", 1);
            message.annotation("ma2", 2);
            message.annotation("ma3", 3);
            // Populate all Properties values
            message.messageId("ID:12345");
            message.userId("user".getBytes(StandardCharsets.UTF_8));
            message.to("the-management");
            message.subject("amqp");
            message.replyTo("the-minions");
            message.correlationId("abc");
            message.contentEncoding("application/json");
            message.contentEncoding("gzip");
            message.absoluteExpiryTime(123);
            message.creationTime(1);
            message.groupId("disgruntled");
            message.groupSequence(8192);
            message.replyToGroupId("/dev/null");
            // Populate message application properties
            message.property("ap1", 1);
            message.property("ap2", 2);
            message.property("ap3", 3);
            // Populate message footers
            message.footer("f1", 1);
            message.footer("f2", 2);
            message.footer("f3", 3);

            // Check preconditions that should affect the send operation
            if (allowAdvancedConversion) {
                assertNotNull(message.toAdvancedMessage());
            } else {
                assertThrows(UnsupportedOperationException.class, () -> message.toAdvancedMessage());
            }

            final Tracker tracker;
            if (trySend) {
                tracker = sender.trySend(message, deliveryAnnotations);
            } else {
                tracker = sender.send(message, deliveryAnnotations);
            }

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendMessageWithMultipleAmqpValueSections() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
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
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            // Note: This is a specification violation but could be used by other message formats
            //       and we don't attempt to enforce at the AdvancedMessage API level what users do.
            EncodedAmqpValueMatcher bodyMatcher1 = new EncodedAmqpValueMatcher("one", true);
            EncodedAmqpValueMatcher bodyMatcher2 = new EncodedAmqpValueMatcher("two", true);
            EncodedAmqpValueMatcher bodyMatcher3 = new EncodedAmqpValueMatcher("three", false);
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.addMessageContentMatcher(bodyMatcher1);
            payloadMatcher.addMessageContentMatcher(bodyMatcher2);
            payloadMatcher.addMessageContentMatcher(bodyMatcher3);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withMessageFormat(17).withPayload(payloadMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final AdvancedMessage<String> message = AdvancedMessage.create();

            message.messageFormat(17);
            message.addBodySection(new AmqpValue<>("one"));
            message.addBodySection(new AmqpValue<>("two"));
            message.addBodySection(new AmqpValue<>("three"));

            final Tracker tracker = sender.send(message);

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendMessageWithMultipleAmqpSequenceSections() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
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
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            List<String> list1 = new ArrayList<>();
            list1.add("1");
            List<String> list2 = new ArrayList<>();
            list2.add("21");
            list2.add("22");
            List<String> list3 = new ArrayList<>();
            list3.add("31");
            list3.add("32");
            list3.add("33");

            EncodedAmqpSequenceMatcher bodyMatcher1 = new EncodedAmqpSequenceMatcher(list1, true);
            EncodedAmqpSequenceMatcher bodyMatcher2 = new EncodedAmqpSequenceMatcher(list2, true);
            EncodedAmqpSequenceMatcher bodyMatcher3 = new EncodedAmqpSequenceMatcher(list3, false);
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.addMessageContentMatcher(bodyMatcher1);
            payloadMatcher.addMessageContentMatcher(bodyMatcher2);
            payloadMatcher.addMessageContentMatcher(bodyMatcher3);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final AdvancedMessage<List<String>> message = AdvancedMessage.create();

            message.addBodySection(new AmqpSequence<>(list1));
            message.addBodySection(new AmqpSequence<>(list2));
            message.addBodySection(new AmqpSequence<>(list3));

            final Tracker tracker = sender.send(message);

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSendMessageWithMultipleDataSections() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
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
            SenderOptions options = new SenderOptions().deliveryMode(DeliveryMode.AT_MOST_ONCE);
            Sender sender = session.openSender("test-qos", options);

            // Gates send on remote flow having been sent and received
            session.openReceiver("dummy").openFuture().get();

            byte[] buffer1 = new byte[] { 1 };
            byte[] buffer2 = new byte[] { 1, 2 };
            byte[] buffer3 = new byte[] { 1, 2, 3 };

            EncodedDataMatcher bodyMatcher1 = new EncodedDataMatcher(buffer1, true);
            EncodedDataMatcher bodyMatcher2 = new EncodedDataMatcher(buffer2, true);
            EncodedDataMatcher bodyMatcher3 = new EncodedDataMatcher(buffer3, false);
            TransferPayloadCompositeMatcher payloadMatcher = new TransferPayloadCompositeMatcher();
            payloadMatcher.addMessageContentMatcher(bodyMatcher1);
            payloadMatcher.addMessageContentMatcher(bodyMatcher2);
            payloadMatcher.addMessageContentMatcher(bodyMatcher3);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.expectTransfer().withPayload(payloadMatcher).accept();
            peer.expectDetach().respond();
            peer.expectClose().respond();

            final AdvancedMessage<byte[]> message = AdvancedMessage.create();

            message.addBodySection(new Data(buffer1));
            message.addBodySection(new Data(buffer2));
            message.addBodySection(new Data(buffer3));

            final Tracker tracker = sender.send(message);

            assertNotNull(tracker);
            assertNotNull(tracker.settlementFuture().isDone());
            assertNotNull(tracker.settlementFuture().get().settled());

            sender.closeAsync().get(10, TimeUnit.SECONDS);

            connection.closeAsync().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
