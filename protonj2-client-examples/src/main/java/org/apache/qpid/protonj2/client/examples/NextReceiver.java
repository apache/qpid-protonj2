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

import java.util.concurrent.ForkJoinPool;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;

public class NextReceiver {

    public static void main(String[] args) throws Exception {
        final String serverHost = System.getProperty("HOST", "localhost");
        final int serverPort = Integer.getInteger("PORT", 5672);
        final String address1 = System.getProperty("ADDRESS1", "next-receiver-1-address");
        final String address2 = System.getProperty("ADDRESS2", "next-receiver-2-address");

        final Client client = Client.create();

        final ConnectionOptions options = new ConnectionOptions();
        options.user(System.getProperty("USER"));
        options.password(System.getProperty("PASSWORD"));

        try (Connection connection = client.connect(serverHost, serverPort, options)) {

            connection.openReceiver(address1);
            connection.openReceiver(address2);

            ForkJoinPool.commonPool().execute(() -> {
                try {
                    Thread.sleep(2000);
                    connection.send(Message.create("Hello World 1").to(address1));
                    Thread.sleep(2000);
                    connection.send(Message.create("Hello World 2").to(address2));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            final Delivery delivery1 = connection.nextReceiver().receive();
            final Delivery delivery2 = connection.nextReceiver().receive();

            System.out.println("Received first message with body: " + delivery1.message().body());
            System.out.println("Received second message with body: " + delivery2.message().body());
        }
    }
}
