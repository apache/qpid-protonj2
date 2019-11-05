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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.messaginghub.amqperative.impl.ClientException;

public interface Sender {

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
     *  </li>
     *  <li>
     *    If neither of the above is true then the address returned is the address passed to the original
     *    {@link Session#openSender(String)} or {@link Session#openSender(String, SenderOptions)} methods.
     *  </li>
     * </ul>
     *
     * @return the address that this {@link Sender} is sending to.
     */
    String address();

    /**
     * @return the {@link Client} instance that holds this session's {@link Sender}
     */
    Client client();

    /**
     * @return the {@link Session} that created and holds this {@link Sender}.
     */
    Session getSession();

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

    Future<Sender> openFuture();

    Future<Sender> close();

    Future<Sender> detach();

    // Ideas not yet worked out.

    Tracker send(Message<?> message, Consumer<Tracker> onUpdated);

    Tracker send(Message<?> message, Consumer<Tracker> onUpdated, ExecutorService executor);

}
