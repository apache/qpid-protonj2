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

import org.messaginghub.amqperative.client.ClientException;

/**
 * Connection
 */
public interface Connection {

    /**
     * @return the {@link Client} instance that holds this {@link Connection}
     */
    Client getClient();

    /**
     * @return a {@link Future} that will be completed when the remote opens this {@link Connection}.
     */
    Future<Connection> openFuture();

    /**
     * @return a {@link Future} that will be completed when the remote opens this {@link Connection}.
     */
    Future<Connection> close();

    /**
     * Creates a receiver used to consumer messages from the given node address.
     *
     * @param address
     *            The source address to attach the consumer to.
     *
     * @return the consumer.
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openReceiver(String address) throws ClientException;

    /**
     * Creates a receiver used to consumer messages from the given node address.
     *
     * @param address
     *            The source address to attach the consumer to.
     * @param receiverOptions
     *            The options for this receiver.
     *
     * @return the consumer.
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openReceiver(String address, ReceiverOptions receiverOptions) throws ClientException;

    /**
     * Returns the default anonymous sender used by this {@link Connection} for {@link #send(Message)}
     * calls.  If the sender has not been created yet this call will initiate its creation and open with
     * the remote peer.
     *
     * @return the sender.
     *
     * @throws ClientException if an internal error occurs.
     */
    Sender defaultSender() throws ClientException;

    /**
     * Creates a sender used to send messages to the given node address.
     *
     * @param address
     *            The target address to attach to, cannot be null.
     *
     * @return the sender.
     *
     * @throws ClientException if an internal error occurs.
     */
    Sender openSender(String address) throws ClientException;

    /**
     * Creates a sender used to send messages to the given node address.
     *
     * @param address
     *            The target address to attach to, cannot be null.
     * @param senderOptions
     *            The options for this sender.
     *
     * @return the sender.
     *
     * @throws ClientException if an internal error occurs.
     */
    Sender openSender(String address, SenderOptions senderOptions) throws ClientException;

    /**
     * Creates a sender that is established to the 'anonymous relay' and as such each
     * message that is sent using this sender must specify an address in its destination
     * address field.
     *
     * @return the sender.
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
     * @return the sender.
     *
     * @throws ClientException if an internal error occurs.
     */
    Sender openAnonymousSender(SenderOptions senderOptions) throws ClientException;

    /**
     * Returns the default {@link Session} instance that is used by this Connection to
     * create the default anonymous connection {@link Sender}.
     *
     * @return a new {@link Session} instance.
     *
     * @throws ClientException if an internal error occurs.
     */
    Session defaultSession() throws ClientException;

    /**
     * Creates a new {@link Session} instance for use by the client application.
     *
     * @return a new {@link Session} instance.
     *
     * @throws ClientException if an internal error occurs.
     */
    Session openSession() throws ClientException;

    /**
     * Creates a new {@link Session} instance for use by the client application.
     *
     * @param options
     *      The {@link SessionOptions} that control properties of the created session.
     *
     * @return a new {@link Session} instance.
     *
     * @throws ClientException if an internal error occurs.
     */
    Session openSession(SessionOptions options) throws ClientException;

    /**
     * Sends the given {@link Message} using the internal connection sender.
     * <p>
     * The connection {@link Sender} is an anonymous AMQP sender which requires that the
     * given message has a valid to value set.
     *
     * @param message
     * 		The message to send
     *
     * @return a {@link Tracker} that allows the client to track settlement of the message.
     *
     * @throws ClientException if an internal error occurs.
     */
    Tracker send(Message<?> message) throws ClientException;

}
