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

import java.io.InputStream;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.StreamDelivery;
import org.apache.qpid.protonj2.client.StreamReceiver;
import org.apache.qpid.protonj2.client.StreamReceiverMessage;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;

public class LargeMessageReceiver {

    public static void main(String[] args) throws Exception {

        try {
            String brokerHost = "localhost";
            int brokerPort = 5672;
            String address = "examples";

            Client client = Client.create();
            Connection connection = client.connect(brokerHost, brokerPort);

            StreamReceiver receiver = connection.openStreamReceiver(address);
            StreamDelivery delivery = receiver.receive();
            StreamReceiverMessage message = delivery.message();
            InputStream inputStream = message.body();

            byte[] chunk = new byte[100];

            int bytesRead = inputStream.read(chunk);
            while (bytesRead != -1) {
                // Process inbound data
            }

            connection.close().get();
        } catch (ClientOperationTimedOutException e) {
            System.out.println("Timed out waiting for message to arrive, exiting.");
            System.exit(1);
        } catch (Exception exp) {
            System.out.println("Caught exception, exiting.");
            exp.printStackTrace(System.out);
            System.exit(1);
        } finally {
        }
    }
}
