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
package org.messaginghub.amqperative.impl;

import org.apache.qpid.proton4j.engine.Session;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.ReceiverOptions;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.SenderOptions;

/**
 *
 */
public class ProtonSession {

    private final ProtonSessionOptions options;
    private final ProtonConnection connection;
    private final Session session;

    public ProtonSession(ProtonSessionOptions options, ProtonConnection connection, Session session) {
        this.options = options;
        this.connection = connection;
        this.session = session;
    }

    /**
     * Creates a new {@link Receiver} instance using the given options and address.
     *
     * @param address
     *      The address to be used when creating the receiver link.
     * @param receiverOptions
     *      The options used to configure the receiver link.
     *
     * @return a new {@link Receiver} instance configured with the given options and address.
     */
    public Receiver createReceiver(String address, ReceiverOptions receiverOptions) {
        return new ProtonReceiver(receiverOptions, this, session.receiver(address));
    }

    /**
     * Creates a new {@link Sender} instance using the given options and address.
     *
     * @param address
     *      The address to be used when creating the receiver link.
     * @param senderOptions
     *      The options used to configure the receiver link.
     *
     * @return a new {@link Sender} instance configured with the given options and address.
     */
    public Sender createSender(String address, SenderOptions senderOptions) {
        return new ProtonSender(senderOptions, this, session.sender(address));
    }
}
