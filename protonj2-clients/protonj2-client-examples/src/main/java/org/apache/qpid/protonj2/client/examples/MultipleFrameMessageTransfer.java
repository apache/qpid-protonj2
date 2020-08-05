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

import java.util.UUID;

import org.apache.qpid.protonj2.client.AdvancedMessage;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.ClientOptions;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.SendContext;
import org.apache.qpid.protonj2.client.Sender;

public class MultipleFrameMessageTransfer {

    public static void main(String[] args) {
        try {
            String brokerHost = "localhost";
            int brokerPort = 5672;
            String address = "examples";

            ClientOptions options = new ClientOptions();
            options.id(UUID.randomUUID().toString());
            Client client = Client.create(options);

            Connection connection = client.connect(brokerHost, brokerPort);
            Sender sender = connection.openSender(address);
            SendContext context = sender.createSendContext();

            // Use an message object to encapsulate the data to be sent, which in this
            // case results in a message with multiple Data Sections.  Using your own
            // AdvancedMessage to control the encoding allows for any message payload
            // to be sent using your own message format.
            AdvancedMessage<byte[]> message = AdvancedMessage.create();
            message.messageFormat(42);

            message.body(new byte[] { 0, 1, 2, 3, 4 });
            context.send(message);

            message.body(new byte[] { 5, 6, 7, 8, 9 });
            context.send(message);

            message.body(new byte[] { 10, 11, 12, 13, 14 });
            context.complete(message);

            connection.close().get();
        } catch (Exception exp) {
            System.out.println("Caught exception, exiting.");
            exp.printStackTrace(System.out);
            System.exit(1);
        } finally {
        }
    }

}
