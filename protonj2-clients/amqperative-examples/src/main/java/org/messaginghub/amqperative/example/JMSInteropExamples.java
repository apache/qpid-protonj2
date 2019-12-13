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

import org.messaginghub.amqperative.Client;
import org.messaginghub.amqperative.Connection;
import org.messaginghub.amqperative.ConnectionOptions;
import org.messaginghub.amqperative.DurabilityMode;
import org.messaginghub.amqperative.ExpiryPolicy;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.ReceiverOptions;

/**
 * Some samples for use in showing how JMS mapping specification interoperability might
 * be handled by the imperative API client.
 */
public class JMSInteropExamples {

    @SuppressWarnings("unused")
    public static void main(String[] args) throws Exception {
        if (args.length < 10) {
            throw new IllegalStateException("This clas isnt meant to be run, its for textual examples usage");
        }

        String brokerHost = "localhost";
        int brokerPort = 5672;
        String address = "examples";

        // =============== Create a connection and request capabilities ===========

        Client client = Client.create();
        ConnectionOptions connectionOpts = new ConnectionOptions();
        connectionOpts.desiredCapabilities("ANONYMOUS_RELAY", "DELIVERY_DELAY", "SHARED_SUBS");
        Connection connection = client.connect(brokerHost, brokerPort).openFuture().get();
        String[] remoteOffered = connection.offeredCapabilities();  // Check what was returned.

        // ============== Create a receiver on a temporary Topic

        ReceiverOptions receiverOptions = new ReceiverOptions();
        receiverOptions.offeredCapabilities("temporary-topic");
        receiverOptions.sourceOptions().durabilityMode(DurabilityMode.NONE);
        receiverOptions.sourceOptions().expiryPolicy(ExpiryPolicy.LINK_CLOSE);

        Receiver dynamicReceiver = connection.openDynamicReceiver().openFuture().get();
        String dynamicAddress = dynamicReceiver.address();

        dynamicReceiver.close();

        // ============== Create a receiver on a temporary Topic

        receiverOptions = new ReceiverOptions();
        receiverOptions.offeredCapabilities("temporary-queue");
        receiverOptions.sourceOptions().durabilityMode(DurabilityMode.NONE);
        receiverOptions.sourceOptions().expiryPolicy(ExpiryPolicy.LINK_CLOSE);

        dynamicReceiver = connection.openDynamicReceiver().openFuture().get();
        dynamicAddress = dynamicReceiver.address();

        dynamicReceiver.close();
    }
}
