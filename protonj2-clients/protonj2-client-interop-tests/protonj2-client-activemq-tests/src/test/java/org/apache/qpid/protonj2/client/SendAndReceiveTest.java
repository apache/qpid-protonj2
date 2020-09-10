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
package org.apache.qpid.protonj2.client;

import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.support.ImperativeClientTestSupport;
import org.apache.qpid.protonj2.client.support.Wait;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
public class SendAndReceiveTest extends ImperativeClientTestSupport {

    @Test
    public void testConcurrentSendAndReceive() throws Exception {
        final int NUM_MESSAGES = 1;
        final URI brokerURI = getBrokerAmqpConnectionURI();
        final CountDownLatch senderDone = new CountDownLatch(NUM_MESSAGES);
        final CountDownLatch receiverDone = new CountDownLatch(NUM_MESSAGES);

        final Executor executor = Executors.newFixedThreadPool(2);

        executor.execute(() -> readMessages(brokerURI.getHost(), brokerURI.getPort(), getTestName(), senderDone));
        executor.execute(() -> sendMessages(brokerURI.getHost(), brokerURI.getPort(), getTestName(), receiverDone));

        senderDone.await();
        receiverDone.await();

        final QueueViewMBean queueView = getProxyToQueue(getTestName());
        Wait.assertEquals(0, () -> queueView.getQueueSize());
    }

    private void sendMessages(String host, int port, String address, CountDownLatch latch) {
        try {
            final Client client = Client.create(new ClientOptions().id("sender-container"));
            final Connection connection = client.connect(host, port);
            final Sender sender = connection.openSender(address);
            final byte[] body = new byte[100];

            Arrays.fill(body, (byte) 120);

            Tracker finalSend = null;

            while (latch.getCount() > 0) {
                final Message<byte[]> message = Message.create(body);
                final long stime = System.currentTimeMillis();

                message.applicationProperty("SendTime", stime);
                message.messageId("ID:" + latch.getCount());

                finalSend = sender.send(message);

                LOG.trace("Sent message:{} which was at: {}", latch.getCount(), stime);

                latch.countDown();
            }

            finalSend.settlementFuture().get();

            LOG.info("Finished sending all messages to address: {}", address);
            connection.close();
        } catch (ClientException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void readMessages(String host, int port, String address, CountDownLatch latch) {
        try {
            final Client client = Client.create(new ClientOptions().id("receiver-container"));
            final Connection connection = client.connect(host, port);
            final Receiver receiver = connection.openReceiver(address, new ReceiverOptions().creditWindow(10));

            while (latch.getCount() > 0) {
                final Delivery delivery = receiver.receive(100, TimeUnit.MILLISECONDS);

                if (delivery == null) {
                    continue;
                }

                Message<byte[]> message = delivery.message();

                latch.countDown();

                final Object id = message.messageId();
                final long stime = (long) message.applicationProperty("SendTime");

                LOG.trace("Read message:{} which was sent at: {}", id, stime);
            }

            LOG.info("Finished reading all messages from address: {}", address);
            connection.close();
        } catch (ClientException e) {
            e.printStackTrace();
        }
    }
}
