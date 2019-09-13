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
package org.messaginghub.amqperative;

import java.util.concurrent.Future;

import org.messaginghub.amqperative.impl.ClientException;

/**
 * Session object used to create {@link Sender} and {@link Receiver} instances.
 */
public interface Session {

    /**
     * @return the {@link Client} instance that holds this session's {@link Connection}
     */
    Client getClient();

    /**
     * @return the {@link Connection} that created and holds this {@link Session}.
     */
    Connection getConnection();

    /**
     * @return a {@link Future} that will be completed when the remote opens this {@link Session}.
     */
    Future<Session> openFuture();

    /**
     * @return a {@link Future} that will be completed when the remote closes this {@link Session}.
     */
    Future<Session> close();

    /**
     * Creates a receiver used to consume messages from the given node address.
     *
     * @param address
     *            The source address to attach the consumer to.
     *
     * @return the newly created {@link Receiver}
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openReceiver(String address) throws ClientException;

    /**
     * Creates a receiver used to consume messages from the given node address.
     *
     * @param address
     *            The source address to attach the consumer to.
     * @param receiverOptions
     *            The options for this receiver.
     *
     * @return the newly created {@link Receiver}
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openReceiver(String address, ReceiverOptions receiverOptions) throws ClientException;

    /**
     * Creates a dynamic receiver used to consume messages from the given node address.
     *
     * @return the newly created {@link Receiver}
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openDynamicReceiver() throws ClientException;

    /**
     * Creates a dynamic receiver used to consume messages from the given node address.
     *
     * @param receiverOptions
     *            The options for this receiver.
     *
     * @return the newly created {@link Receiver}
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openDynamicReceiver(ReceiverOptions receiverOptions) throws ClientException;

    /**
     * Creates a sender used to send messages to the given node address. If no
     * address (i.e null) is specified then a sender will be established to the
     * 'anonymous relay' and each message must specify its destination address.
     *
     * @param address
     *            The target address to attach to, or null to attach to the
     *            anonymous relay.
     *
     * @return the newly created {@link Sender}.
     *
     * @throws ClientException if an internal error occurs.
     */
    Sender openSender(String address) throws ClientException;

    /**
     * Creates a sender used to send messages to the given node address. If no
     * address (i.e null) is specified then a sender will be established to the
     * 'anonymous relay' and each message must specify its destination address.
     *
     * @param address
     *            The target address to attach to, or null to attach to the
     *            anonymous relay.
     * @param senderOptions
     *            The options for this sender.
     *
     * @return the newly created {@link Sender}.
     *
     * @throws ClientException if an internal error occurs.
     */
    Sender openSender(String address, SenderOptions senderOptions) throws ClientException;

    /**
     * Creates a sender that is established to the 'anonymous relay' and as such each
     * message that is sent using this sender must specify an address in its destination
     * address field.
     *
     * @return the newly created {@link Sender}.
     *
     * @throws ClientException if an internal error occurs.
     */
    Sender openAnonymousSender() throws ClientException;

    /**
     * Creates a sender that is established to the 'anonymous relay' and as such each
     * message that is sent using this sender must specify an address in its destination
     * address field.
     *
     * @param senderOptions
     *            The options for this sender.
     *
     * @return the newly created {@link Sender}.
     *
     * @throws ClientException if an internal error occurs.
     */
    Sender openAnonymousSender(SenderOptions senderOptions) throws ClientException;

}
