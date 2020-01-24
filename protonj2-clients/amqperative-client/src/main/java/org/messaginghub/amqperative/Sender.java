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

public interface Sender {

    /**
     * @return a {@link Future} that will be completed when the remote opens this {@link Sender}.
     */
    Future<Sender> openFuture();

    /**
     * Requests a close of the {@link Sender} link at the remote and returns a {@link Future} that will be
     * completed once the link has been closed.
     *
     * @return a {@link Future} that will be completed when the remote closes this {@link Sender} link.
     */
    Future<Sender> close();

    /**
     * Requests a close of the {@link Sender} link at the remote and returns a {@link Future} that will be
     * completed once the link has been closed.
     *
     * @param error
     * 		The {@link ErrorCondition} to transmit to the remote along with the close operation.
     *
     * @return a {@link Future} that will be completed when the remote closes this {@link Sender} link.
     */
    Future<Sender> close(ErrorCondition error);

    /**
     * Requests a detach of the {@link Sender} link at the remote and returns a {@link Future} that will be
     * completed once the link has been detached.
     *
     * @return a {@link Future} that will be completed when the remote detaches this {@link Sender} link.
     */
    Future<Sender> detach();

    /**
     * Requests a detach of the {@link Sender} link at the remote and returns a {@link Future} that will be
     * completed once the link has been detached.
     *
     * @param error
     * 		The {@link ErrorCondition} to transmit to the remote along with the detach operation.
     *
     * @return a {@link Future} that will be completed when the remote detaches this {@link Sender} link.
     */
    Future<Sender> detach(ErrorCondition error);

    /**
     * Returns the address that the {@link Sender} instance will send {@link Message} objects
     * to.  The value returned from this method is control by the configuration that was used
     * to create the sender.
     *
     * <ul>
     *  <li>
     *    If the Sender is configured as an anonymous sender then this method returns null.
     *  </li>
     *  <li>
     *    If the Sender was created with the dynamic sender methods then the method will return
     *    the dynamically created address once the remote has attached its end of the sender link.
     *    Due to the need to await the remote peer to populate the dynamic address this method will
     *    block until the open of the sender link has completed.
     *  </li>
     *  <li>
     *    If neither of the above is true then the address returned is the address passed to the original
     *    {@link Session#openSender(String)} or {@link Session#openSender(String, SenderOptions)} methods.
     *  </li>
     * </ul>
     *
     * @return the address that this {@link Sender} is sending to.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Sender} address.
     */
    String address() throws ClientException;

    /**
     * Returns an immutable view of the remote {@link Source} object assigned to this sender link.  If the
     * attach has not completed yet this method will block to await the attach response which carries the remote
     * {@link Source}.
     *
     * @return the remote {@link Source} node configuration.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Sender} remote {@link Source}.
     */
    Source source() throws ClientException;

    /**
     * Returns an immutable view of the remote {@link Target} object assigned to this sender link.  If the
     * attach has not completed yet this method will block to await the attach response which carries the remote
     * {@link Target}.
     *
     * @return the remote {@link Target} node configuration.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Sender} remote {@link Target}.
     */
    Target target() throws ClientException;

    /**
     * Returns the properties that the remote provided upon successfully opening the {@link Sender}.  If the
     * attach has not completed yet this method will block to await the attach response which carries the remote
     * properties.  If the remote provides no properties this method will return null.
     *
     * @return any properties provided from the remote once the sender has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Sender} remote properties.
     */
    Map<String, Object> properties() throws ClientException;

    /**
     * Returns the offered capabilities that the remote provided upon successfully opening the {@link Sender}.
     * If the attach has not completed yet this method will block to await the attach response which carries the
     * remote offered capabilities.  If the remote provides no capabilities this method will return null.
     *
     * @return any capabilities provided from the remote once the sender has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Sender} remote offered capabilities.
     */
    String[] offeredCapabilities() throws ClientException;

    /**
     * Returns the desired capabilities that the remote provided upon successfully opening the {@link Sender}.
     * If the attach has not completed yet this method will block to await the attach response which carries the
     * remote desired capabilities.  If the remote provides no capabilities this method will return null.
     *
     * @return any desired capabilities provided from the remote once the sender has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Sender} remote desired capabilities.
     */
    String[] desiredCapabilities() throws ClientException;

    /**
     * @return the {@link Client} instance that holds this session's {@link Sender}
     */
    Client client();

    /**
     * @return the {@link Session} that created and holds this {@link Sender}.
     */
    Session session();

    /**
     * Send the given message.
     *
     * @param message
     *      the {@link Message} to send.
     *
     * @return the {@link Tracker} for the message delivery
     *
     * @throws ClientException if an error occurs while initiating the send operation.
     */
    Tracker send(Message<?> message) throws ClientException;

    /**
     * Send the given message if credit is available.
     *
     * @param message
     *      the {@link Message} to send if credit is available.
     *
     * @return the {@link Tracker} for the message delivery or null if no credit for sending.
     *
     * @throws ClientException if an error occurs while initiating the send operation.
     */
    Tracker trySend(Message<?> message) throws ClientException;

}
