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
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;

public class ReconnectReceiver {

    private static final int MESSAGE_COUNT = 10;

    private static String serverHost = "localhost";
    private static int serverPort = 5672;
    private static String address = "reconnect-receiver-examples";

    private static String backupServerHost = System.getProperty("backup_server_host");
    private static int backupServerPort = Integer.getInteger("backup_server_port", 5672);

    public static void main(String[] args) throws Exception {
        Client client = Client.create();
        ConnectionOptions connectionOpts = new ConnectionOptions();
        connectionOpts.reconnectEnabled(true);

        if (backupServerHost != null) {
            connectionOpts.reconnectOptions().addReconnectLocation(backupServerHost, backupServerPort);
        }

        try (Connection connection = client.connect(serverHost, serverPort, connectionOpts);
             Receiver receiver = connection.openReceiver(address)) {

            for (int receivedCount = 0; receivedCount < MESSAGE_COUNT; ++receivedCount) {
                Delivery delivery = receiver.receive();
                Message<String> message = delivery.message();
                System.out.print(message.body());
            }
        } catch (Exception exp) {
            System.out.println("Caught exception, exiting.");
            exp.printStackTrace(System.out);
            System.exit(1);
        }
    }
}
