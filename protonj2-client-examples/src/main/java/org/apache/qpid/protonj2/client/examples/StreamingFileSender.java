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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.StreamSender;
import org.apache.qpid.protonj2.client.StreamSenderMessage;

/**
 * Sends the file given in argument zero to the remote address 'file-transfer'
 */
public class StreamingFileSender {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Example requires a valid file name to transfer");
            System.exit(1);
        }

        final File inputFile = new File(args[0]);
        if (!inputFile.exists() || !inputFile.canRead()) {
            System.out.println("Example requires a valid / readable file to transfer");
            System.exit(1);
        }

        String fileNameKey = "filename";
        String serverHost = System.getProperty("server_host", "localhost");
        int serverPort = Integer.getInteger("server_port", 5672);
        String address = System.getProperty("address", "file-transfer");

        Client client = Client.create();

        try (Connection connection = client.connect(serverHost, serverPort);
             StreamSender sender = connection.openStreamSender(address);
             FileInputStream inputStream = new FileInputStream(inputFile)) {

            StreamSenderMessage message = sender.beginMessage();

            // Inform the other side what the original file name was.
            message.property(fileNameKey, inputFile.getName());

            // Creates an OutputStream that writes the file in smaller data sections which allows for
            // larger file sizes than the single AMQP Data section bounded configuration might allow.
            // When not specifying a body size the application will need to close the output to indicate
            // the transfer is complete, here we use a try with resources approach to accomplish that.
            try (OutputStream output = message.body()) {
                // Let the streams handle the actual transfer which will block until the full transfer
                // is complete, or if an error occurs either in the file reader or the stream sender
                // the message send should be aborted.
                inputStream.transferTo(output);
            } catch (IOException ioe) {
                message.abort();
            }

            message.tracker().awaitSettlement();
        }
    }
}
