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
package org.apache.qpid.proton4j.engine;

import java.nio.ByteBuffer;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * API for an outgoing Delivery.
 */
public interface OutgoingDelivery extends Delivery {

    /**
     * @return the link that this {@link Delivery} is bound to.
     */
    @Override
    Sender getLink();

    // TODO - How to complete or send partial delivery

    // OutgoingDelivery flush();  write what was written for partial send

    // OutgoingDelivery complete();  write what was written as complete transfer

    // TODO - names of these send methods, send, sendBytes, write, writeBytes ?

    /**
     * @param buffer
     *      The buffer whose contents should be sent.
     *
     * @throws {@link IllegalStateException} if the current credit prohibits sending the requested amount of bytes
     */
    void writeBytes(ProtonBuffer buffer);

    /**
     * @param buffer
     *      The buffer whose contents should be sent.
     *
     * @throws {@link IllegalStateException} if the current credit prohibits sending the requested amount of bytes
     */
    void writeBytes(ByteBuffer buffer);

    /**
     * @param array
     *      The array whose contents should be sent
     * @param offset
     *      The offset into the array to start sending from
     * @param length
     *      The number of bytes in the array that should be sent
     *
     * @throws {@link IllegalStateException} if the current credit prohibits sending the requested amount of bytes
     */
    void writeBytes(byte[] array, int offset, int length);

    /**
     * Sets the message-format for this Delivery, representing the 32bit value using an int.
     *
     * The default value is 0 as per the message format defined in the core AMQP 1.0 specification.<p>
     *
     * See the following for more details:<br>
     * <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-transfer">
     *          http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-transfer</a><br>
     * <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-message-format">
     *          http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-message-format</a><br>
     * <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#section-message-format">
     *          http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#section-message-format</a><br>
     * <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#definition-MESSAGE-FORMAT">
     *          http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#definition-MESSAGE-FORMAT</a><br>
     *
     * @param messageFormat the message format
     *
     * @return this outgoing delivery instance.
     */
    public OutgoingDelivery setMessageFormat(int messageFormat);

    /**
     * Aborts the outgoing delivery if not already settled.
     *
     * @return this delivery.
     */
    OutgoingDelivery abort();

}
