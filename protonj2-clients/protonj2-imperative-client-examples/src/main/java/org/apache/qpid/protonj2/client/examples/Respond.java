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
package org.apache.qpid.protonj2.client.examples;

import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.Sender;

/**
 * Listens for Requests on a request Queue and sends a response.
 */
public class Respond {

    public static void main(String[] args) throws Exception {
        String serverHost = "localhost";
        int serverPort = 5672;
        String address = "examples";

        Client client = Client.create();

        try (Connection connection = client.connect(serverHost, serverPort)) {
            ReceiverOptions receiverOptions = new ReceiverOptions();
            receiverOptions.sourceOptions().capabilities("queue");

            Receiver receiver = connection.openReceiver(address, receiverOptions);

            Delivery request = receiver.receive(30, TimeUnit.SECONDS);
            if (request != null) {
                Message<String> received = request.message();
                System.out.println("Received message with body: " + received.body());

                String replyAddress = received.replyTo();
                if (replyAddress != null) {
                    Sender sender = connection.openSender(address);
                    sender.send(Message.create("Response"));
                }
            } else {
                System.out.println("Failed to read a message during the defined wait interval.");
            }
        }
    }
}
