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

import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.qpid.proton4j.amqp.driver.netty.NettyTestPeer;
import org.junit.Ignore;
import org.junit.Test;
import org.messaginghub.amqperative.client.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for the Connection class
 */
public class ConnectionTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionTest.class);

    @Test(timeout = 60000)
    public void testCreateConnectionString() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            Container container = Container.create();
            LOG.info("Created container");

            Connection connection = container.createConnection(remoteURI.getHost(), remoteURI.getPort());
            LOG.info("Connection creation started (or already failed), waiting.");

            connection.openFuture().get(10, TimeUnit.SECONDS);
            LOG.info("Open completed successfully");

            connection.close().get(10, TimeUnit.SECONDS);
            LOG.info("Close completed successfully");

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCreateConnectionURI() throws Exception {
    }

    @Ignore("Skipped for now, needs server, and proton changes")//TODO
    @Test
    public void testSendAndReceiveMessage() throws InterruptedException, ExecutionException, TimeoutException, ClientException {
        Container container = Container.create();
        System.out.println("Created container");

        Connection conn = container.createConnection("localhost", 5672);
        System.out.println("Connection creation started (or already failed), waiting.");

        int timeout = 300;
        conn.openFuture().get(timeout, TimeUnit.SECONDS);
        System.out.println("Open completed successfully");

        ReceiverOptions receiverOptions = new ReceiverOptions().setCreditWindow(10);

        Receiver receiver = conn.createReceiver("queue", receiverOptions);
        receiver.openFuture().get(timeout, TimeUnit.SECONDS);

        Sender sender = conn.createSender("queue");

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
