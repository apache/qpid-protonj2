/*
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
 */
package org.apache.qpid.protonj2.client.examples.reconnect;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Sender;

public class ReconnectSender {

    private static final int MESSAGE_COUNT = 10;

    public static void main(String[] args) throws Exception {
        final String serverHost = System.getProperty("HOST", "localhost");
        final int serverPort = Integer.getInteger("PORT", 5672);
        final String address = System.getProperty("ADDRESS", "reconnect-examples");
        final String backupServerHost = System.getProperty("BACKUP_HOST");
        final int backupServerPort = Integer.getInteger("BACKUP_PORT", 5672);

        final Client client = Client.create();

        final ConnectionOptions connectionOpts = new ConnectionOptions();
        connectionOpts.user(System.getProperty("USER"));
        connectionOpts.password(System.getProperty("PASSWORD"));
        connectionOpts.reconnectEnabled(true);

        if (backupServerHost != null) {
            connectionOpts.reconnectOptions().addReconnectLocation(backupServerHost, backupServerPort);
        }

        try (Connection connection = client.connect(serverHost, serverPort, connectionOpts);
             Sender sender = connection.openSender(address)) {

            for (int sentCount = 1; sentCount <= MESSAGE_COUNT; ) {
                try {
                    sender.send(Message.create("Hello World: #" + sentCount)).awaitAccepted();
                    sentCount++;
                } catch (Exception ex) {
                    System.out.println("Caught exception during send, will retry on next connect");
                }
            }
        } catch (Exception exp) {
            System.out.println("Caught exception, exiting.");
            exp.printStackTrace(System.out);
            System.exit(1);
        }
    }
}
