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

import static org.hamcrest.Matchers.notNullValue;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.transport.AMQPHeader;
import org.apache.qpid.protonj2.test.driver.utils.TestPeerTestsBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test various aspects of how the test peer handles and responds to transactional work.
 */
@Timeout(20)
class TransactionHandlingTest extends TestPeerTestsBase {

    private static final Logger LOG = LoggerFactory.getLogger(SessionHandlingTest.class);

    @Test
    public void testCoordinatorAttachAndDetachHandling() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().ofSender().respond();
            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofReceiver().onChannel(0).withHandle(0);
            client.expectDetach();
            client.expectEnd();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender()
                                 .withCoordinator().also()
                                 .withSource().withAddress("txn-address")
                                 .and().now();
            client.remoteDetach().now();
            client.remoteEnd().now();
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testRemoteDetachLastCoordiantorLink() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().ofSender().respond();
            peer.remoteDetachLastCoordinatorLink().queue();
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofReceiver().onChannel(0).withHandle(0);
            client.expectDetach();
            client.expectEnd();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender()
                                 .withCoordinator().also()
                                 .withSource().withAddress("txn-address")
                                 .and().now();
            client.remoteEnd().now();
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testRemoteDetachLastCoordiantorLinkAttachesFirstIfNeeded() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().ofSender();
            peer.remoteDetachLastCoordinatorLink().queue();
            peer.expectDetach();
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectAttach().ofReceiver()
                                 .onChannel(0)
                                 .withHandle(0)
                                 .withSource(notNullValue())
                                 .withTarget(notNullValue())
                                 .withName("txn");
            client.expectDetach().respond();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender()
                                 .withName("txn")
                                 .withCoordinator().also()
                                 .withSource().withAddress("txn-address")
                                 .and().now();
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectEnd();
            client.remoteEnd().now();
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testTxnDeclarationAndDischargeNullMessageFormat() throws Exception {
        doTestTxnDeclarationAndDischarge(null);
    }

    @Test
    public void testTxnDeclarationAndDischargeZeroMessageFormat() throws Exception {
        doTestTxnDeclarationAndDischarge(UnsignedInteger.ZERO);
    }

    private void doTestTxnDeclarationAndDischarge(UnsignedInteger messageFormat) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
             ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectCoordinatorAttach().ofSender().respond();
            peer.remoteFlow().withLinkCredit(2).queue();
            peer.expectDeclare().withMessageFormat(messageFormat).declared(new byte[] { 0, 1, 2, 3 });
            peer.expectDischarge().accept();
            peer.expectDetach().respond();
            peer.expectEnd().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin();
            client.expectCoordinatorAttach().ofReceiver();

            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender()
                                 .withName("txn")
                                 .withCoordinator().also()
                                 .withSource().withAddress("txn-address")
                                 .and().now();
            client.expectFlow().withLinkCredit(2);
            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectDisposition().withState().declared(new byte[] {0, 1, 2, 3});
            client.remoteDeclare().withMessageFormat(messageFormat).withDeliveryTag(new byte[] {0}).withDeliveryId(0).now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.expectDisposition().withState().accepted();
            client.expectDetach();
            client.expectEnd();

            client.remoteDischarge().withDeliveryId(1).withDeliveryTag(new byte[] {1}).now();
            client.remoteDetach().now();
            client.remoteEnd().now();

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            client.close();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
