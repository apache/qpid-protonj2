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
package org.apache.qpid.protonj2.client.examples;

import java.io.OutputStream;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.StreamSender;
import org.apache.qpid.protonj2.client.StreamSenderMessage;

// TODO: Possibly make an advanced folder for the more complex AMQP messaging topics
public class RawLargeMessageSender {

    public static void main(String[] args) throws Exception {

        try {
            String brokerHost = "localhost";
            int brokerPort = 5672;
            String address = "examples";

            Client client = Client.create();

            Connection connection = client.connect(brokerHost, brokerPort);
            StreamSender sender = connection.openStreamSender(address);
            StreamSenderMessage message = sender.beginMessage();

            final byte[] buffer = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

            message.messageFormat(42);

            // Create an OutputStream that will send what should be AMQP encoded data
            // or some other custom message formatted payload.
            OutputStream output = message.rawOutputStream();

            // Write the payload and then close which completes the streamed transfer.
            output.write(buffer);
            output.close();

            connection.close().get();
        } catch (Exception exp) {
            System.out.println("Caught exception, exiting.");
            exp.printStackTrace(System.out);
            System.exit(1);
        } finally {
        }
    }
}
