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
package org.apache.qpid.protonj2.engine;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.types.DeliveryTag;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * API for an outgoing Delivery.
 */
public interface OutgoingDelivery {

    /**
     * @return the link that this {@link OutgoingDelivery} is bound to.
     */
    Sender getLink();

    /**
     * @return the {@link Attachments} instance that is associated with this {@link Delivery}
     */
    Attachments getAttachments();

    /**
     * Links a given resource to this {@link Endpoint}.
     *
     * @param resource
     *      The resource to link to this {@link Endpoint}.
     *
     * @return this {@link OutgoingDelivery} instance.
     */
    OutgoingDelivery setLinkedResource(Object resource);

    /**
     * @return the user set linked resource for this {@link Endpoint} instance.
     */
    <T> T getLinkedResource();

    /**
     * Gets the linked resource (if set) and returns it using the type information
     * provided to cast the returned value.
     *
     * @param <T> The type to cast the linked resource to if one is set.
     * @param typeClass the type's Class which is used for casting the returned value.
     *
     * @return the user set linked resource for this Context instance.
     *
     * @throws ClassCastException if the linked resource cannot be cast to the type requested.
     */
    <T> T getLinkedResource(Class<T> typeClass);

    /**
     * Gets the message-format for this Delivery, representing the 32bit value using an int.
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
     * @return the message-format for this Delivery.
     */
    int getMessageFormat();

    /**
     * Sets the message-format for this Delivery, representing the 32bit value using an integer value. The message format can
     * only be set prior to the first {@link Transfer} of delivery payload having been written.  If one of the delivery write
     * methods is called prior to the message format being set then it defaults to the AMQP default format of zero.
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
     * @return this {@link OutgoingDelivery} instance.
     *
     * @throws IllegalStateException if the delivery has already written {@link Transfer} frames.
     */
    OutgoingDelivery setMessageFormat(int messageFormat);

    /**
     * @return the {@link DeliveryTag} assigned to this Delivery.
     */
    DeliveryTag getTag();

    /**
     * Sets the delivery tag to assign to this outgoing delivery from the given byte array.
     *
     * @param deliveryTag
     *      a byte array containing the delivery tag to assign to this {@link OutgoingDelivery}
     *
     * @return this {@link OutgoingDelivery} instance.
     *
     * @throws IllegalStateException if the delivery has already written {@link Transfer} frames.
     */
    OutgoingDelivery setTag(byte[] deliveryTag);

    /**
     * Sets the {@link DeliveryTag} to assign to this outgoing delivery.
     *
     * @param deliveryTag
     *      a byte array containing the delivery tag to assign to this {@link OutgoingDelivery}
     *
     * @return this {@link OutgoingDelivery} instance.
     */
    OutgoingDelivery setTag(DeliveryTag deliveryTag);

    /**
     * Check for whether the delivery is still partial.
     *
     * For a receiving Delivery, this means the delivery does not hold
     * a complete message payload as all the content hasn't been
     * received yet. Note that an {@link #isAborted() aborted} delivery
     * will also be considered partial and the full payload won't
     * be received.
     *
     * For a sending Delivery, this means that the application has not marked
     * the delivery as complete yet.
     *
     * @return true if the delivery is partial
     *
     * @see #isAborted()
     */
    boolean isPartial();

    /**
     * Write the given bytes as the payload of this delivery, no additional writes can occur on this delivery,
     * <p>
     * When called the provided buffer is treated as containing the entirety of the transfer payload and the
     * Transfer(s) that result from this call will result in a final Transfer frame whose more flag is set to
     * false which tells the remote that no additional data will be sent for this {@link Transfer}.
     *
     * @param buffer
     *      The buffer whose contents should be sent.
     *
     * @return this {@link OutgoingDelivery} instance.
     *
     * TODO - Decide how we handle not being able to write some or all of the bytes
     *
     * @throws IllegalStateException if the current credit prohibits sending the requested amount of bytes
     */
    OutgoingDelivery writeBytes(ProtonBuffer buffer);

    /**
     * Write the given bytes as a portion of the payload of this delivery, additional bytes can be streamed until
     * the stream complete flag is set to true on a call to {@link #streamBytes(ProtonBuffer, boolean)} or a call
     * to {@link #writeBytes(ProtonBuffer)} is made.
     *
     * This method is the same as calling {@link #streamBytes(ProtonBuffer, boolean)} with the complete value set
     * to false.
     *
     * @param buffer
     *      The buffer whose contents should be sent.
     *
     * @return this {@link OutgoingDelivery} instance.
     *
     * TODO - Decide how we handle not being able to write some or all of the bytes
     *
     * @throws IllegalStateException if the current credit prohibits sending the requested amount of bytes
     */
    OutgoingDelivery streamBytes(ProtonBuffer buffer);

    /**
     * Write the given bytes as a portion of the payload of this delivery, additional bytes can be streamed until
     * the stream complete flag is set to true on a call to {@link #streamBytes(ProtonBuffer, boolean)}
     *
     * @param buffer
     *      The buffer whose contents should be sent.
     * @param complete
     *      When true the delivery is marked complete and no further bytes can be written.
     *
     * @return this {@link OutgoingDelivery} instance.
     *
     * TODO - Decide how we handle not being able to write some or all of the bytes
     *
     * @throws IllegalStateException if the current credit prohibits sending the requested amount of bytes
     */
    OutgoingDelivery streamBytes(ProtonBuffer buffer, boolean complete);

    /**
     * @return true if the delivery has been aborted.
     */
    boolean isAborted();

    /**
     * Aborts the outgoing delivery if not already settled.
     *
     * @return this delivery.
     */
    OutgoingDelivery abort();

    /**
     * @return the {@link DeliveryState} at the local side of this Delivery.
     */
    DeliveryState getState();

    /**
     * updates the state of the delivery
     *
     * @param state the new delivery state
     *
     * @return this {@link OutgoingDelivery} instance.
     */
    OutgoingDelivery disposition(DeliveryState state);

    /**
     * Update the delivery with the given disposition if not locally settled
     * and optionally settles the delivery if not already settled.
     *
     * TODO - Fully document the result of this call.
     *
     * @param state
     *      the new delivery state
     * @param settle
     *       if true the delivery is settled.
     *
     * @return this {@link OutgoingDelivery} instance.
     */
    OutgoingDelivery disposition(DeliveryState state, boolean settle);

    /**
     * @return true if the delivery has been settled locally.
     */
    boolean isSettled();

    /**
     * Settles this delivery.
     *
     * TODO - Fully document the result of this call.
     *
     * @return this {@link OutgoingDelivery} instance.
     */
    OutgoingDelivery settle();

    /**
     * @return true if the delivery has been settled by the remote.
     */
    boolean isRemotelySettled();

    /**
     * @return the {@link DeliveryState} at the remote side of this Delivery.
     */
    DeliveryState getRemoteState();

}
