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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.OutputStreamOptions;
import org.apache.qpid.protonj2.client.StreamSender;
import org.apache.qpid.protonj2.client.StreamSenderMessage;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.test.driver.matchers.transport.TransferPayloadCompositeMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedDataMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests that validate Stream Sender behavior after a client reconnection.
 */
@Timeout(20)
class ReconnectStreamSenderTest extends ImperativeClientTestCase {

    @Test
    void testStreamMessageFlushFailsAfterConnectionDropped() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin().respond();
            firstPeer.expectAttach().ofSender().respond();
            firstPeer.remoteFlow().withLinkCredit(1).queue();
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.expectBegin().respond();
            finalPeer.expectAttach().ofSender().respond();
            finalPeer.remoteFlow().withLinkCredit(1).queue();
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
            StreamSender sender = connection.openStreamSender("test-queue");
            StreamSenderMessage message = sender.beginMessage();

            OutputStream stream = message.body();

            EncodedDataMatcher dataMatcher1 = new EncodedDataMatcher(new byte[] { 0, 1, 2, 3 });
            TransferPayloadCompositeMatcher payloadMatcher1 = new TransferPayloadCompositeMatcher();
            payloadMatcher1.setMessageContentMatcher(dataMatcher1);

            EncodedDataMatcher dataMatcher2 = new EncodedDataMatcher(new byte[] { 4, 5, 6, 7 });
            TransferPayloadCompositeMatcher payloadMatcher2 = new TransferPayloadCompositeMatcher();
            payloadMatcher2.setMessageContentMatcher(dataMatcher2);

            firstPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            firstPeer.expectTransfer().withPayload(payloadMatcher1).withMore(true);
            firstPeer.expectTransfer().withPayload(payloadMatcher2).withMore(true);
            firstPeer.dropAfterLastHandler();

            // Write two then after connection drops the message should fail on future writes
            stream.write(new byte[] { 0, 1, 2, 3 });
            stream.flush();
            stream.write(new byte[] { 4, 5, 6, 7 });
            stream.flush();

            firstPeer.waitForScriptToComplete();
            // Reconnection should have occurred now and we should not be able to flush data from
            // the stream as its initial sender instance was closed on disconnect.
            finalPeer.waitForScriptToComplete();
            finalPeer.expectClose().respond();

            // Next write should fail as connection should have dropped.
            stream.write(new byte[] { 8, 9, 10, 11 });

            try {
                stream.flush();
                fail("Should not be able to flush after connection drop");
            } catch (IOException ioe) {
                assertTrue(ioe.getCause() instanceof ClientException);
            }

            connection.closeAsync().get();

            finalPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testStreamMessageCloseThatFlushesFailsAfterConnectionDropped() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

           firstPeer.expectSASLAnonymousConnect();
           firstPeer.expectOpen().respond();
           firstPeer.expectBegin().respond();
           firstPeer.expectAttach().ofSender().respond();
           firstPeer.remoteFlow().withLinkCredit(1).queue();
           firstPeer.start();

           finalPeer.expectSASLAnonymousConnect();
           finalPeer.expectOpen().respond();
           finalPeer.expectBegin().respond();
           finalPeer.expectAttach().ofSender().respond();
           finalPeer.remoteFlow().withLinkCredit(1).queue();
           finalPeer.start();

           final URI primaryURI = firstPeer.getServerURI();
           final URI backupURI = finalPeer.getServerURI();

           ConnectionOptions options = new ConnectionOptions();
           options.reconnectOptions().reconnectEnabled(true);
           options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());

           Client container = Client.create();
           Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
           StreamSender sender = connection.openStreamSender("test-queue");
           StreamSenderMessage message = sender.beginMessage();

            OutputStream stream = message.body();

            EncodedDataMatcher dataMatcher1 = new EncodedDataMatcher(new byte[] { 0, 1, 2, 3 });
            TransferPayloadCompositeMatcher payloadMatcher1 = new TransferPayloadCompositeMatcher();
            payloadMatcher1.setMessageContentMatcher(dataMatcher1);

            EncodedDataMatcher dataMatcher2 = new EncodedDataMatcher(new byte[] { 4, 5, 6, 7 });
            TransferPayloadCompositeMatcher payloadMatcher2 = new TransferPayloadCompositeMatcher();
            payloadMatcher2.setMessageContentMatcher(dataMatcher2);

            firstPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            firstPeer.expectTransfer().withPayload(payloadMatcher1).withMore(true);
            firstPeer.expectTransfer().withPayload(payloadMatcher2).withMore(true);
            firstPeer.dropAfterLastHandler();

            // Write two then after connection drops the message should fail on future writes
            stream.write(new byte[] { 0, 1, 2, 3 });
            stream.flush();
            stream.write(new byte[] { 4, 5, 6, 7 });
            stream.flush();

            firstPeer.waitForScriptToComplete();

            // Reconnection should have occurred now and we should not be able to flush data from
            // the stream as its initial sender instance was closed on disconnect.
            finalPeer.waitForScriptToComplete();
            finalPeer.expectClose().respond();

            // Next write should fail as connection should have dropped.
            stream.write(new byte[] { 8, 9, 10, 11 });

            try {
                stream.close();
                fail("Should not be able to close after connection drop");
            } catch (IOException ioe) {
                assertTrue(ioe.getCause() instanceof ClientException);
            }

            connection.closeAsync().get();

            finalPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testStreamMessageWriteThatFlushesFailsAfterConnectionDropped() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin().respond();
            firstPeer.expectAttach().ofSender().respond();
            firstPeer.remoteFlow().withLinkCredit(1).queue();
            firstPeer.dropAfterLastHandler();
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.expectBegin().respond();
            finalPeer.expectAttach().ofSender().respond();
            finalPeer.remoteFlow().withLinkCredit(1).queue();
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            ConnectionOptions options = new ConnectionOptions();
            options.maxFrameSize(32768);
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
            StreamSender sender = connection.openStreamSender("test-queue");
            StreamSenderMessage message = sender.beginMessage();

            byte[] payload = new byte[65536];
            Arrays.fill(payload, (byte) 65);
            OutputStreamOptions streamOptions = new OutputStreamOptions().bodyLength(payload.length);
            OutputStream stream = message.body(streamOptions);

            firstPeer.waitForScriptToComplete();

            // Reconnection should have occurred now and we should not be able to flush data from
            // the stream as its initial sender instance was closed on disconnect.
            finalPeer.waitForScriptToComplete();
            finalPeer.expectClose().respond();

            try {
                stream.write(payload);
                fail("Should not be able to write section after connection drop");
            } catch (IOException ioe) {
                assertTrue(ioe.getCause() instanceof ClientException);
            }

            connection.closeAsync().get();

            finalPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testStreamSenderRecoveredAfterReconnectCanCreateAndStreamBytes() throws Exception {
        try (ProtonTestServer firstPeer = new ProtonTestServer();
             ProtonTestServer finalPeer = new ProtonTestServer()) {

            firstPeer.expectSASLAnonymousConnect();
            firstPeer.expectOpen().respond();
            firstPeer.expectBegin().respond();
            firstPeer.expectAttach().ofSender().respond();
            firstPeer.remoteFlow().withLinkCredit(1).queue();
            firstPeer.dropAfterLastHandler();
            firstPeer.start();

            finalPeer.expectSASLAnonymousConnect();
            finalPeer.expectOpen().respond();
            finalPeer.expectBegin().respond();
            finalPeer.expectAttach().ofSender().respond();
            finalPeer.remoteFlow().withLinkCredit(1).queue();
            finalPeer.start();

            final URI primaryURI = firstPeer.getServerURI();
            final URI backupURI = finalPeer.getServerURI();

            ConnectionOptions options = new ConnectionOptions();
            options.reconnectOptions().reconnectEnabled(true);
            options.reconnectOptions().addReconnectLocation(backupURI.getHost(), backupURI.getPort());

            Client container = Client.create();
            Connection connection = container.connect(primaryURI.getHost(), primaryURI.getPort(), options);
            StreamSender sender = connection.openStreamSender("test-queue");

            firstPeer.waitForScriptToComplete();

            // After reconnection a new stream sender message should be properly created
            finalPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            finalPeer.expectTransfer().withMore(true).withPayload(new byte[] { 0, 1, 2, 3 });
            finalPeer.expectTransfer().withMore(false).withNullPayload();
            finalPeer.expectDetach().respond();
            finalPeer.expectEnd().respond();
            finalPeer.expectClose().respond();

            StreamSenderMessage message = sender.beginMessage();
            OutputStream stream = message.rawOutputStream();

            stream.write(new byte[] { 0, 1, 2, 3 });
            stream.flush();
            stream.close();

            sender.closeAsync().get();
            connection.closeAsync().get();

            finalPeer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
