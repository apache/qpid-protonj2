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
import java.util.Arrays;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.OutputStreamOptions;
import org.apache.qpid.protonj2.client.StreamSender;
import org.apache.qpid.protonj2.client.StreamSenderMessage;

public class LargeMessageSender {

    public static void main(String[] args) throws Exception {
        String serverHost = "localhost";
        int serverPort = 5672;
        String address = "large-message-example";

        Client client = Client.create();

        try (Connection connection = client.connect(serverHost, serverPort)) {
            StreamSender sender = connection.openStreamSender(address);
            StreamSenderMessage message = sender.beginMessage();

            final byte[] buffer = new byte[100];
            Arrays.fill(buffer, (byte) 'A');

            message.durable(true);

            // Creates an OutputStream that writes a single Data Section whose expected
            // size is configured in the stream options.
            OutputStreamOptions streamOptions = new OutputStreamOptions().bodyLength(buffer.length);
            OutputStream output = message.body(streamOptions);

            final int chunkSize = 10;

            for (int i = 0; i < buffer.length; i += chunkSize) {
                output.write(buffer, i, chunkSize);
            }

            output.close();  // This completes the message send.

            message.tracker().awaitSettlement();
        }
    }
}
