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

    // TODO - Work out how to manage send of data and pending changes, we need to allow for
    //        push of current written bytes without sending a transfer with the more flag
    //        set to false as that precludes streaming more bytes later.

    /**
     * Flush all pending changes to the Delivery but leave the delivery as the active
     * delivery on the link so that additional bytes can be written into this delivery.
     *
     * Flushing a message that was marked as settled has the same affect as calling
     * complete on the message.
     *
     * @return this outgoing delivery instance.
     */
    OutgoingDelivery flush();

    /**
     * Flushes all pending changes for this delivery and notifies the link that a new
     * delivery can now started.  Once marked as complete a delivery will not accept any
     * new bytes written to it however if left unsettled the delivery can be later marked
     * settled and or have its dissipation updated.
     *
     * @return this outgoing delivery instance.
     */
    OutgoingDelivery complete();

    /**
     * @param buffer
     *      The buffer whose contents should be sent.
     *
     * @throws {@link IllegalStateException} if the current credit prohibits sending the requested amount of bytes
     */
    void writeBytes(ProtonBuffer buffer);

    /**
     * Writes the given bytes into this delivery appending them to any previously written but not
     * sent bytes in this delivery.
     *
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
