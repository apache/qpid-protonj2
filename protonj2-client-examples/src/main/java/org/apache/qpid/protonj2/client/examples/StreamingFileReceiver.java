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
import java.io.FileOutputStream;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.StreamDelivery;
import org.apache.qpid.protonj2.client.StreamReceiver;
import org.apache.qpid.protonj2.client.StreamReceiverMessage;

/**
 * Receives a streamed file and writes it to the path given on the command line.
 */
public class StreamingFileReceiver {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Example requires a valid directory where the incoming file should be written");
            System.exit(1);
        }

        final File outputPath = new File(args[0]);
        if (!outputPath.isDirectory() || !outputPath.canWrite()) {
            System.out.println("Example requires a valid / writable directory to transfer to");
            System.exit(1);
        }

        String fileNameKey = "filename";
        String serverHost = System.getProperty("server_host", "localhost");
        int serverPort = Integer.getInteger("server_port", 5672);
        String address = System.getProperty("address", "file-transfer");

        Client client = Client.create();

        try (Connection connection = client.connect(serverHost, serverPort);
             StreamReceiver receiver = connection.openStreamReceiver(address)) {

            StreamDelivery delivery = receiver.receive();
            StreamReceiverMessage message = delivery.message();

            // The remote should have told us the filename of the original file it sent.
            String filename = (String) message.applicationProperty(fileNameKey);
            if (filename == null || filename.isBlank()) {
                System.out.println("Remote did not include the source filename in the incoming message");
                System.exit(1);
            } else {
                System.out.println("Starting receive of incoming file named: " + filename);
            }

            FileOutputStream outputStream = new FileOutputStream(new File(outputPath, filename));

            message.body().transferTo(outputStream);

            outputStream.close();
        }
    }
}
