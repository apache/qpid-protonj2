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

import org.messaginghub.amqperative.impl.ClientException;

/**
 * Incoming Delivery type that provides access to the message and the delivery
 * data along with methods for settling the delivery when processing completes.
 */
public interface Delivery {

    /**
     * @return the {@link Receiver} that originated this {@link Delivery}.
     */
    Receiver getReceiver();

    /**
     * Decode the {@link Delivery} payload and return an {@link Message} object.
     *
     * @return a {@link Message} instance that wraps the decoded payload.
     *
     * @throws ClientException if an error occurs while decoding the payload.
     *
     * @param <E> The type of message body that should be contained in the returned {@link Message}.
     */
    <E> Message<E> getMessage() throws ClientException;

    // TODO: Expose means of reading delivery bytes possibly with support for partial reads/
    //       Need to define how we expose partial messages if at all and how to allow uses to
    //       read / write those.  The above would likely have to throw a specific error if the
    //       users is handed an incomplete message.
    //       If adding such support the read and getMessage objects should be made mutually exclusive.
    // readAll(OutputStream stream);
    // readAll(ByteBuffer buffer);

    /**
     * Accepts and settles the delivery.
     *
     * @return itself
     */
    Delivery accept();

    //TODO: other specific disposition helpers, or just direct to the below disposition catch-all?

    /**
     * Updates the DeliveryState, and optionally settle the delivery as well.
     *
     * @param state
     *            the delivery state to apply
     * @param settle
     *            whether to {@link #settle()} the delivery at the same time
     * @return itself
     */
    Delivery disposition(DeliveryState state, boolean settle);

    /**
     * Settles the delivery locally.
     *
     * @return the delivery
     */
    Delivery settle();

    /**
     * @return true if the delivery has been locally settled.
     */
    boolean isSettled();

    /**
     * Gets the current local state for the delivery.
     *
     * @return the delivery state
     */
    DeliveryState getState();

    /**
     * Gets the current remote state for the delivery.
     *
     * @return the remote delivery state
     */
    DeliveryState getRemoteState();

    // TODO: Hide this not so useful and probably to low level API in a new
    //       message interface or other construct for advanced AMQP bits

    /**
     * Gets whether the delivery was settled by the remote peer yet.
     *
     * @return whether the delivery is remotely settled
     */
    boolean isRemotelySettled();

    /**
     * Gets the delivery tag for this delivery
     *
     * @return the tag
     */
    byte[] getTag();

    /**
     * Gets the message format for the current delivery.
     *
     * @return the message format
     */
    int getMessageFormat();

}