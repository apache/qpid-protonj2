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
package org.apache.qpid.protonj2.client;

import java.util.Map;

import org.apache.qpid.protonj2.client.exceptions.ClientException;

/**
 * Sending link implementation that allows sending of large message payload data in
 * multiple transfers to reduce memory overhead of large message sends.
 */
public interface StreamSender extends Link<StreamSender> {

    /**
     * Send the given message immediately if there is credit available or blocks if the link
     * has not yet been granted credit or there is a streaming send ongoing.
     * <p>
     * Upon successfully sending the message the methods returns a {@link Tracker} that can
     * be used to await settlement of the message from the remote.  If the sender has been
     * configured to send the message pre-settled then the resulting Tracker will immediately
     * report the message as remotely settlement and accepted.
     *
     * @param message
     *      the {@link Message} to send.
     *
     * @return the {@link Tracker} for the message delivery
     *
     * @throws ClientException if an error occurs while initiating the send operation.
     */
    StreamTracker send(Message<?> message) throws ClientException;

    /**
     * Send the given message immediately if there is credit available or blocks if the link
     * has not yet been granted credit or there is a streaming send ongoing. The provided
     * delivery annotations are encoded along with the message, the annotations can be passed
     * repeatedly to send calls if sending the same delivery annotations with each message.
     * <p>
     * Upon successfully sending the message the methods returns a {@link Tracker} that can
     * be used to await settlement of the message from the remote.  If the sender has been
     * configured to send the message pre-settled then the resulting Tracker will immediately
     * report the message as remotely settlement and accepted.
     *
     * @param message
     *      the {@link Message} to send.
     * @param deliveryAnnotations
     *      the delivery annotations that should be included in the sent {@link Message}.
     *
     * @return the {@link StreamTracker} for the message delivery
     *
     * @throws ClientException if an error occurs while initiating the send operation.
     */
    StreamTracker send(Message<?> message, Map<String, Object> deliveryAnnotations) throws ClientException;

    /**
     * Send the given message if credit is available or returns null if no credit has been
     * granted to the link at the time of the send attempt or a streaming send is ongoing.
     * <p>
     * Upon successfully sending the message the methods returns a {@link Tracker} that can
     * be used to await settlement of the message from the remote.  If the sender has been
     * configured to send the message pre-settled then the resulting Tracker will immediately
     * report the message as remotely settlement and accepted.
     *
     * @param message
     *      the {@link Message} to send if credit is available.
     *
     * @return the {@link StreamTracker} for the message delivery or null if no credit for sending.
     *
     * @throws ClientException if an error occurs while initiating the send operation.
     */
    StreamTracker trySend(Message<?> message) throws ClientException;

    /**
     * Send the given message if credit is available or returns null if no credit has been
     * granted to the link at the time of the send attempt. The provided delivery annotations
     * are encoded along with the message, the annotations can be passed repeatedly to send
     * calls if sending the same delivery annotations with each message.
     * <p>
     * Upon successfully sending the message the methods returns a {@link Tracker} that can
     * be used to await settlement of the message from the remote.  If the sender has been
     * configured to send the message pre-settled then the resulting Tracker will immediately
     * report the message as remotely settlement and accepted.
     *
     * @param message
     *      the {@link Message} to send if credit is available.
     * @param deliveryAnnotations
     *      the delivery annotations that should be included in the sent {@link Message}.
     *
     * @return the {@link StreamTracker} for the message delivery or null if no credit for sending.
     *
     * @throws ClientException if an error occurs while initiating the send operation.
     */
    StreamTracker trySend(Message<?> message, Map<String, Object> deliveryAnnotations) throws ClientException;

    /**
     * Creates and returns a new {@link StreamSenderMessage} that can be used by the caller to perform
     * streaming sends of large message payload data.
     *
     * @return a new {@link StreamSenderMessage} that can be used to stream message data to the remote.
     *
     * @throws ClientException if an error occurs while initiating a new streaming send message.
     */
    StreamSenderMessage beginMessage() throws ClientException;

    /**
     * Creates and returns a new {@link StreamSenderMessage} that can be used by the caller to perform
     * streaming sends of large message payload data. The provided delivery annotations are encoded
     * along with the message, the annotations can be passed repeatedly to send calls if sending the
     * same delivery annotations with each message.
     *
     * @param deliveryAnnotations
     *      the delivery annotations that should be included in the sent {@link StreamSenderMessage}.
     *
     * @return a new {@link StreamSenderMessage} that can be used to stream message data to the remote.
     *
     * @throws ClientException if an error occurs while initiating a new streaming send message.
     */
    StreamSenderMessage beginMessage(Map<String, Object> deliveryAnnotations) throws ClientException;

}
