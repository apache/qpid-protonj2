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

import java.util.Map;
import java.util.concurrent.Future;

import org.messaginghub.amqperative.impl.ClientException;

/**
 * Top level {@link Connection} object that can be used as a stand alone API for sending
 * messages and creating {@link Receiver} instances for message consumption. The Connection
 * API also exposes a {@link Session} based API for more advanced messaging use cases.
 *
 * When a Connection is closed all the resources created by the connection are implicitly closed.
 */
public interface Connection {

    // TODO - API docs should;d better reflect the implied state of the returned objects
    //        like senders, receivers and session where the might not yet be remotely opened.

    /**
     * @return the {@link Client} instance that holds this {@link Connection}
     */
    Client client();

    /**
     * When a {@link Connection} is created it may not be opened on the remote peer, the future returned
     * from this method allows the caller to await the completion of the Connection open by the remote before
     * proceeding on to other messaging operations.  If the open of the connection fails at the remote an
     * {@link Exception} is thrown from the {@link Future#get()} method when called.
     *
     * @return a {@link Future} that will be completed when the remote opens this {@link Connection}.
     */
    Future<Connection> openFuture();

    /**
     * Requests a close of the {@link Connection} at the remote and returns a {@link Future} that will be
     * completed once the Connection has been fully closed.
     *
     * @return a {@link Future} that will be completed when the remote closes this {@link Connection}.
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
     * @param dynamicNodeProperties
     * 		The dynamic node properties to be applied to the node created by the remote.
     *
     * @return the newly created {@link Receiver}
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openDynamicReceiver(Map<String, Object> dynamicNodeProperties) throws ClientException;

    /**
     * Creates a dynamic receiver used to consume messages from the given node address.
     *
     * @param receiverOptions
     * 		The options for this receiver.
     *
     * @return the newly created {@link Receiver}
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openDynamicReceiver(ReceiverOptions receiverOptions) throws ClientException;

    /**
     * Creates a dynamic receiver used to consume messages from the given node address.
     *
     * @param dynamicNodeProperties
     * 		The dynamic node properties to be applied to the node created by the remote.
     * @param receiverOptions
     *      The options for this receiver.
     *
     * @return the newly created {@link Receiver}
     *
     * @throws ClientException if an internal error occurs.
     */
    Receiver openDynamicReceiver(Map<String, Object> dynamicNodeProperties, ReceiverOptions receiverOptions) throws ClientException;

    // TODO: Does this verify if the server supports anonymous senders and throw if they don't?
    // TODO: Why have both send + defaultSender methods? To allow for waiting for the attach/open to complete before send?
    // TODO: If it gets closed, do we create a new one and have this return the updated one?

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

    // TODO: Does this verify if the server supports anonymous senders and throw if they don't? Try and fail?
    // (Well, actually, that would succeed since it isn't remotely opened on return...)
    // Need to define what the exceptional cases are vs the going to fail later on wait cases.

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
     * create the default anonymous connection {@link Sender} as well as creating those
     * resources created from the {@link Connection} such as {@link Sender} and {@link Receiver}
     * instances not married to a specific {@link Session}.
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

    // TODO: Does this verify if the server supports anonymous senders and throw if they don't?
    //       the contract isn't made entirely clear here and needs to be made more explicit.

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

    /**
     * Returns the properties that the remote provided upon successfully opening the {@link Connection}.
     *
     * Until the remote opens the connection the value returned from this method will be null, only once successfully
     * connected will this method return a non-null Map value if the remote returned any properties otherwise it will
     * continue to return null.
     *
     * @return any properties provided from the remote once the connection has successfully opened.
     */
    Map<String, Object> properties();

    /**
     * Returns the capabilities that the remote offers upon successfully opening the {@link Connection}.
     *
     * Until the remote opens the connection the value returned from this method will be null, only once successfully
     * connected will this method return a non-null string array value if the remote returned any offered capabilities
     * otherwise it will continue to return null.
     *
     * @return any capabilities provided from the remote once the connection has successfully opened.
     */
    String[] offeredCapabilities();

    /**
     * Returns the capabilities that the remote desires upon successfully opening the {@link Connection}.
     *
     * Until the remote opens the connection the value returned from this method will be null, only once successfully
     * connected will this method return a non-null string array value if the remote returned any desired capabilities
     * otherwise it will continue to return null.
     *
     * @return any desired capabilities provided from the remote once the connection has successfully opened.
     */
    String[] desiredCapabilities();

}
