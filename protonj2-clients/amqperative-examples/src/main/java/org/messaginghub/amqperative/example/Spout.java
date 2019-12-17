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
package org.messaginghub.amqperative.example;

import java.util.UUID;

import org.messaginghub.amqperative.Client;
import org.messaginghub.amqperative.ClientOptions;
import org.messaginghub.amqperative.Connection;
import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.Tracker;

public class Spout {

    public static void main(String[] argv) throws Exception {

        String brokerHost = "localhost";
        int brokerPort = 5672;
        String address = "examples";
        int count = 100;

        ClientOptions options = new ClientOptions();
        options.containerId(UUID.randomUUID().toString());
        Client client = Client.create(options);

        try {
            Connection connection = client.connect(brokerHost, brokerPort);
            Sender sender = connection.openSender(address);

            for (int i = 0; i < count; ++i) {
                Message<String> message = Message.create(String.format("Hello World! [%s]", i));

                Tracker tracker = sender.send(message);  // Blocks on credit

                System.out.println(
                    String.format("Sent %s to %s: %s",
                            new String(tracker.tag()), sender.address(), message.body()));

                tracker.acknowledgeFuture().get();
            }

            connection.close().get();
        } catch (Exception exp) {
            System.out.println("Caught exception, exiting.");
            exp.printStackTrace(System.out);
            System.exit(1);
        } finally {
        }
    }
}
