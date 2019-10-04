/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.messaginghub.amqperative.example;

import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.messaginghub.amqperative.Client;
import org.messaginghub.amqperative.ClientOptions;
import org.messaginghub.amqperative.Connection;
import org.messaginghub.amqperative.ConnectionOptions;
import org.messaginghub.amqperative.Delivery;
import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.ReceiverOptions;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.SenderOptions;
import org.messaginghub.amqperative.Tracker;
import org.messaginghub.amqperative.impl.ClientException;
import org.messaginghub.amqperative.TerminusOptions.DurabilityMode;
import org.messaginghub.amqperative.TerminusOptions.ExpiryPolicy;

public class Samples {

    public static void main(String[] args) throws Exception {
        if(args.length < 10) {
            throw new IllegalStateException("This clas isnt meant to be run, its for textual examples usage");
        }


        String brokerHost = "localhost";
        int brokerPort = 5672;
        String address = "examples";

        // =============== Create a client ===========
        Client client = Client.create(); // No-arg

        ClientOptions options = new ClientOptions();
        options.setContainerId(UUID.randomUUID().toString());
        Client client2 = Client.create(options); // With options


        // =============== Create a connection ===========

        Connection connection = client.connect(brokerHost, brokerPort); // host + port

        Connection connection2 = client.connect(brokerHost); // host only (port defaulted [maybe configurable at client?])

        ConnectionOptions connectionOptions = new ConnectionOptions();
        connectionOptions.setUser("myUsername");
        connectionOptions.setPassword("myPassword");
        Connection connection3 = client.connect(brokerHost, brokerPort, connectionOptions); // host + port + options

        Connection connection4 = client.connect(brokerHost, connectionOptions); // host + options

        // =============== Create a sender ===========

        Sender sender = connection.openSender(address); //address-only
        sender.openFuture().get(5, TimeUnit.SECONDS);

        SenderOptions senderOptions = new SenderOptions();
        senderOptions.getTarget().setCapabilities(new String[]{"topic"});
        senderOptions.setSendTimeout(30_000);
        Sender sender2 = connection.openSender(address, senderOptions); // address and options

        // =============== Send a message ===========

        Message<String> message = Message.create("Hello World").setDurable(true);
        Tracker tracker = sender.send(message);

        tracker.settle();

        // =============== Create a receiver ===========

        Receiver receiver = connection.openReceiver(address); //address-only
        receiver.openFuture().get(5, TimeUnit.SECONDS);

        ReceiverOptions receiverOptions = new ReceiverOptions();
        //receiverOptions.setCreditWindow(10);
        receiverOptions.setLinkName("myLinkName");
        receiverOptions.getSource().setDurabilityMode(DurabilityMode.CONFIGURATION);
        receiverOptions.getSource().setExpiryPolicy(ExpiryPolicy.NEVER);
        receiverOptions.getSource().setCapabilities(new String[]{"topic"});
        Receiver receiver2 = connection.openReceiver(address, receiverOptions); // address and options

        // =============== Receive a message ===========

        receiver.addCredit(1); // Or configure a credit window (see above)


        Delivery delivery = receiver.receive(); // Waits forever
        if (delivery != null) {
            Message<String> received = delivery.getMessage();

            System.out.println(received.getBody());
        }

        Delivery delivery2 = receiver.receive(5_000); // Waits with timeout

        Delivery delivery3 = receiver.tryReceive(); // Return delivery if available, null if not.


        receiver.onMessage(delivery4 -> {
            try {
                Message<String> received = delivery.getMessage();
                System.out.println(received.getBody());
            } catch (ClientException e) {
                // TODO Make a RuntimeException
                e.printStackTrace();
            }
        }, ForkJoinPool.commonPool());


        delivery.accept(); // Or configure auto-accept?

        // =============== Close/ detach ===========

        Future<Sender> closeFuture = sender.close();
        closeFuture.get(5, TimeUnit.SECONDS);

        Future<Receiver> detachFuture = receiver.detach();
        detachFuture.get(5, TimeUnit.SECONDS);

        Future<Client> closeClientFuture = client.close();
        closeClientFuture.get(5, TimeUnit.SECONDS);
    }
}
