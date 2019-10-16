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
package org.messaginghub.amqperative;

import static org.junit.Assert.fail;

import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.qpid.proton4j.amqp.driver.netty.NettyTestPeer;
import org.apache.qpid.proton4j.amqp.transport.ConnectionError;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.junit.Ignore;
import org.junit.Test;
import org.messaginghub.amqperative.impl.ClientException;
import org.messaginghub.amqperative.test.AMQPerativeTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for the Connection class
 */
public class ConnectionTest extends AMQPerativeTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionTest.class);

    @Test(timeout = 60000)
    public void testCreateConnectionString() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            // TODO - Wrong frame should trigger connection drop so that
            //        the waits for open / close etc will fail quickly.
            // peer.expectBegin().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);
            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            LOG.info("Connect test completed normally");
        }
    }

    @Test(timeout = 60000)
    public void testConnectionCloseGetsResponseWithErrorDoesNotThrow() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectClose().respond().
                               withErrorCondition(new ErrorCondition(ConnectionError.CONNECTION_FORCED, "Not accepting connections"));
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);
            // Should close normally and not throw error as we initiated the close.
            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            LOG.info("Connect test completed normally");
        }
    }

    @Test(timeout = 60000)
    public void testConnectionRemoteClosedAfterOpened() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.remoteClose().withErrorCondition(
                    new ErrorCondition(ConnectionError.CONNECTION_FORCED, "Not accepting connections")).queue();
            peer.expectClose();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);

            LOG.info("Connect test completed normally");
        }
    }

    @Test(timeout = 60000)
    public void testConnectionOpenFutureWaitCancelledOnConnectionDrop() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
            peer.close();

            try {
                connection.openFuture().get(10, TimeUnit.SECONDS);
                fail("Should have thrown an execution error due to connection drop");
            } catch (ExecutionException error) {
                LOG.info("connection open failed with error: ", error);
            }

            try {
                connection.close().get(10, TimeUnit.SECONDS);
            } catch (Throwable error) {
                LOG.info("connection close failed with error: ", error);
                fail("Close should ignore connect error and complete without error.");
            }

            LOG.info("Connect test completed normally");
        }
    }

    @Ignore("Skipped for now, needs server, and proton changes")//TODO
    @Test
    public void testSendAndReceiveMessage() throws InterruptedException, ExecutionException, TimeoutException, ClientException {
        Client container = Client.create();
        System.out.println("Created container");

        Connection conn = container.connect("localhost", 5672);
        System.out.println("Connection creation started (or already failed), waiting.");

        int timeout = 300;
        conn.openFuture().get(timeout, TimeUnit.SECONDS);
        System.out.println("Open completed successfully");

        ReceiverOptions receiverOptions = new ReceiverOptions().setCreditWindow(10);

        Receiver receiver = conn.openReceiver("queue", receiverOptions);
        receiver.openFuture().get(timeout, TimeUnit.SECONDS);

        Sender sender = conn.openSender("queue");

        sender.openFuture().get(timeout, TimeUnit.SECONDS);
        System.out.println("Sender created successfully");

        Thread.sleep(200);//TODO: remove, hack to allow sender to become sendable first.

        int count = 100;
        for (int i = 1; i <= count; i++) {
            //TODO: This fails if a prev message wasn't locally settled yet (which I'm deliberately not doing here,
            //      instead tweaked proton current() method to use !isPartial() rather than isSettled(..but that causes test failures))
            Tracker tracker = sender.send(Message.create("myBasicTextMessage" + i));
            System.out.println("Sent message " + i);

            Delivery delivery = receiver.receive(1000);

            if (delivery == null) {
                throw new IllegalStateException("Expected delivery but did not get one");
            }

            System.out.println("Got message body: " + delivery.getMessage().getBody());

            delivery.accept();

            Thread.sleep(20); //TODO: remove, hack to give time for settlement propagation (when send+receive done end to end via dispatch router)
            System.out.println("Settled: " + tracker.isRemotelySettled());
            //TODO: should locally settle sent delivery..if sender not set to 'auto settle' when peer does.
        }

        Future<Connection> closing = conn.close();
        System.out.println("Close started, waiting.");

        closing.get(3, TimeUnit.SECONDS);
        System.out.println("Close completed");
    }
}
