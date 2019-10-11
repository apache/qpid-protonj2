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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.messaginghub.amqperative.Client;
import org.messaginghub.amqperative.ClientOptions;
import org.messaginghub.amqperative.Connection;
import org.messaginghub.amqperative.Delivery;
import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.Tracker;

public class HelloWorld {

    public static void main(String[] args) throws Exception {

        try {
            String brokerHost = "localhost";
            int brokerPort = 5672;
            String address = "examples";

            ClientOptions options = new ClientOptions();
            options.setContainerId(UUID.randomUUID().toString());
            Client client = Client.create(options);

            Connection connection = client.connect(brokerHost, brokerPort);

            Receiver receiver = connection.openReceiver(address);
            receiver.openFuture().get(5, TimeUnit.SECONDS);
            receiver.addCredit(10);

            Sender sender = connection.openSender(address);
            sender.openFuture().get(5, TimeUnit.SECONDS);

            Message<String> message = Message.create("Hello World").setDurable(true);
            Tracker tracker = sender.send(message);

            Delivery delivery = receiver.receive();

            if (delivery != null) {
                Message<String> received = delivery.getMessage();

                System.out.println(received.getBody());
            }

            Future<Receiver> draining = receiver.drain();
            draining.get(5, TimeUnit.SECONDS);

            connection.close().get(5, TimeUnit.SECONDS);
        } catch (Exception exp) {
            System.out.println("Caught exception, exiting.");
            exp.printStackTrace(System.out);
            System.exit(1);
        } finally {
        }
    }
}
