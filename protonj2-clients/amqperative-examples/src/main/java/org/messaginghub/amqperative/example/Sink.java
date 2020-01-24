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
import org.messaginghub.amqperative.Delivery;
import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.Receiver;

public class Sink {

    public static void main(String[] argv) throws Exception {
        String brokerHost = "localhost";
        int brokerPort = 5672;
        String address = "examples";
        int count = 100;

        ClientOptions options = new ClientOptions();
        options.id(UUID.randomUUID().toString());
        Client client = Client.create(options);

        try {
            Connection connection = client.connect(brokerHost, brokerPort);
            Receiver receiver = connection.openReceiver(address);

            for (int i = 0; i < count; ++i) {
                Delivery del = receiver.receive();
                Message<String> message = del.message();

                System.out.println("Received: " + message.body());

                del.accept();
            }
        } catch (Exception exp) {
            System.out.println("Caught exception, exiting.");
            exp.printStackTrace(System.out);
            System.exit(1);
        } finally {
            client.close().get();
        }
    }
}
