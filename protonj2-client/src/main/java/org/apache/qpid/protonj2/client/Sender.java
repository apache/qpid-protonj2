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
 * AMQP Sender that provides an API for sending complete Message payload data.
 */
public interface Sender extends Link<Sender> {

    /**
     * Send the given message immediately if there is credit available or blocks if the link
     * has not yet been granted credit.
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
     * Send the given message immediately if there is credit available or blocks if the link
     * has not yet been granted credit.
     *
     * @param message
     *      the {@link Message} to send.
     * @param deliveryAnnotations
     *      the delivery annotations that should be included in the sent {@link Message}.
     *
     * @return the {@link Tracker} for the message delivery
     *
     * @throws ClientException if an error occurs while initiating the send operation.
     */
    Tracker send(Message<?> message, Map<String, Object> deliveryAnnotations) throws ClientException;

    /**
     * Send the given message if credit is available or returns null if no credit has been
     * granted to the link at the time of the send attempt.
     *
     * @param message
     *      the {@link Message} to send if credit is available.
     *
     * @return the {@link Tracker} for the message delivery or null if no credit for sending.
     *
     * @throws ClientException if an error occurs while initiating the send operation.
     */
    Tracker trySend(Message<?> message) throws ClientException;

    /**
     * Send the given message if credit is available or returns null if no credit has been
     * granted to the link at the time of the send attempt.
     *
     * @param message
     *      the {@link Message} to send if credit is available.
     * @param deliveryAnnotations
     *      the delivery annotations that should be included in the sent {@link Message}.
     *
     * @return the {@link Tracker} for the message delivery or null if no credit for sending.
     *
     * @throws ClientException if an error occurs while initiating the send operation.
     */
    Tracker trySend(Message<?> message, Map<String, Object> deliveryAnnotations) throws ClientException;

}
