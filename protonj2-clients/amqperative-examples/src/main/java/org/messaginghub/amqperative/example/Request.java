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
import java.util.concurrent.TimeUnit;

import org.messaginghub.amqperative.Client;
import org.messaginghub.amqperative.ClientOptions;
import org.messaginghub.amqperative.Connection;
import org.messaginghub.amqperative.Delivery;
import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.SenderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends a Request to a request Queue and awaits a response.
 */
public class Request {

    private static final Logger LOG = LoggerFactory.getLogger(Request.class);

    public static void main(String[] args) throws Exception {

        String brokerHost = "localhost";
        int brokerPort = 5672;
        String address = "examples";

        ClientOptions options = new ClientOptions();
        options.containerId(UUID.randomUUID().toString());
        Client client = Client.create(options);

        try {
            Connection connection = client.connect(brokerHost, brokerPort);

            Receiver dynamicReceiver = connection.openDynamicReceiver();
            dynamicReceiver.openFuture().get(5, TimeUnit.SECONDS);  // Waiting for remote to provide address
            String dynamicAddress = dynamicReceiver.address();
            System.out.println("Waiting for response to requests on address: " + dynamicAddress);

            SenderOptions requestorOptions = new SenderOptions();
            requestorOptions.targetOptions().capabilities("queue");

            Sender requestor = connection.openSender(address, requestorOptions);
            Message<String> request = Message.create("Hello World").durable(true).replyTo(dynamicAddress);
            requestor.send(request);

            dynamicReceiver.addCredit(1);
            Delivery response = dynamicReceiver.receive(30_000);
            Message<String> received = response.message();
            LOG.info("Response to request message was: {}", received.body());
        } catch (Exception exp) {
            LOG.error("Caught exception during Request demo, exiting.", exp);
            System.exit(1);
        } finally {
            client.close().get(5, TimeUnit.SECONDS);
        }
    }
}
