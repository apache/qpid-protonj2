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
import org.apache.qpid.protonj2.types.transport.Disposition;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * API for an incoming Delivery.
 */
public interface IncomingDelivery {

    /**
     * @return the link that this {@link IncomingDelivery} is bound to.
     */
    Receiver getLink();

    /**
     * Returns the number of bytes currently available for reading form this delivery, which may not be complete yet.
     * <p>
     * Note that this value will change as bytes are received, and is in general not equal to the total length of
     * a delivery, except the point where {@link #isPartial()} returns false and no content has yet been received by
     * the application.
     *
     * @return the number of bytes currently available to read from this delivery.
     */
    int available();

    /**
     * Marks all available bytes as being claimed by the caller meaning that available byte count value can
     * be returned to the session which can expand the session incoming window to allow more bytes to be
     * sent from the remote peer.
     * <p>
     * This method is useful in the case where the {@link Session} has been configured with a small incoming
     * capacity and the receiver needs to expand the session window in order to read the entire contents of
     * a delivery whose payload exceeds the configured session capacity.  The {@link IncomingDelivery}
     * implementation will track the amount of claimed bytes and ensure that it never releases back more
     * bytes to the {@link Session} than has actually been received as a whole which allows this method
     * to be called with each incoming {@link Transfer} frame of a large split framed delivery.
     *
     * @return this {@link IncomingDelivery} instance.
     */
    IncomingDelivery claimAvailableBytes();

    /**
     * Returns the current read buffer without copying it effectively consuming all currently available
     * bytes from this delivery.  If no data is available then this method returns <code>null</code>.
     *
     * @return the currently available read bytes for this delivery.
     */
    ProtonBuffer readAll();

    /**
     * Reads bytes from this delivery and writes them into the destination ProtonBuffer reducing the available
     * bytes by the value of the number of bytes written to the target. The number of bytes written will be the
     * equal to the writable bytes of the target buffer. The write index of the target buffer will be incremented
     * by the number of bytes written into it.
     *
     * @param buffer
     *      The target buffer that will be written into.
     *
     * @return this {@link IncomingDelivery} instance.
     *
     * @throws IndexOutOfBoundsException if the target buffer has more writable bytes than this delivery has readable bytes.
     */
    IncomingDelivery readBytes(ProtonBuffer buffer);

    /**
     * Reads bytes from this delivery and writes them into the destination array starting at the given offset and
     * continuing for the specified length reducing the available bytes by the value of the number of bytes written
     * to the target.
     *
     * @param array
     *      The target buffer that will be written into.
     * @param offset
     *      The offset into the given array to begin writing.
     * @param length
     *      The number of bytes to write to the given array.
     *
     * @return this {@link IncomingDelivery} instance.
     *
     * @throws IndexOutOfBoundsException if the length is greater than this delivery has readable bytes.
     */
    IncomingDelivery readBytes(byte[] array, int offset, int length);

    /**
     * Configures a default DeliveryState to be used if a received delivery is settled/freed
     * without any disposition state having been previously applied.
     *
     * @param state the default delivery state
     *
     * @return this {@link IncomingDelivery} instance.
     */
    IncomingDelivery setDefaultDeliveryState(DeliveryState state);

    /**
     * @return the default delivery state for this delivery
     */
    DeliveryState getDefaultDeliveryState();

    /**
     * @return the {@link Attachments} instance that is associated with this {@link IncomingDelivery}
     */
    Attachments getAttachments();

    /**
     * Links a given resource to this {@link IncomingDelivery}.
     *
     * @param resource
     *      The resource to link to this {@link IncomingDelivery}.
     *
     * @return this {@link IncomingDelivery} instance.
     */
    IncomingDelivery setLinkedResource(Object resource);

    /**
     * @param <T> The type that the linked resource should be cast to on return.
     *
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
     * @return the {@link DeliveryTag} assigned to this Delivery.
     */
    DeliveryTag getTag();

    /**
     * @return the {@link DeliveryState} at the local side of this Delivery.
     */
    DeliveryState getState();

    /**
     * Gets the message-format for this Delivery, representing the 32bit value using an int.
     * <p>
     * The default value is 0 as per the message format defined in the core AMQP 1.0 specification.
     * <p>
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
     * Check for whether the delivery is still partial.
     * <p>
     * For a receiving Delivery, this means the delivery does not hold
     * a complete message payload as all the content hasn't been
     * received yet. Note that an {@link #isAborted() aborted} delivery
     * will also be considered partial and the full payload won't
     * be received.
     * <p>
     * For a sending Delivery, this means that the application has not marked
     * the delivery as complete yet.
     *
     * @return true if the delivery is partial
     *
     * @see #isAborted()
     */
    boolean isPartial();

    /**
     * @return true if the delivery has been aborted.
     */
    boolean isAborted();

    /**
     * @return true if the delivery has been settled locally.
     */
    boolean isSettled();

    /**
     * updates the state of the delivery
     *
     * @param state the new delivery state
     *
     * @return this {@link IncomingDelivery} instance.
     */
    IncomingDelivery disposition(DeliveryState state);

    /**
     * Update the delivery with the given disposition if not locally settled
     * and optionally settles the delivery if not already settled.
     * <p>
     * Applies the given delivery state and local settlement value to this delivery
     * writing a new {@link Disposition} frame if the remote has not already settled
     * the delivery.  Once locally settled no additional updates to the local
     * {@link DeliveryState} can be applied and if attempted an {@link IllegalStateException}
     * will be thrown to indicate this is not possible.
     *
     * @param state
     *      the new delivery state
     * @param settle
     *       if true the delivery is settled.
     *
     * @return this {@link IncomingDelivery} instance.
     */
    IncomingDelivery disposition(DeliveryState state, boolean settle);

    /**
     * Settles this delivery locally, transmitting a {@link Disposition} frame to the remote
     * if the remote has not already settled the delivery.  Once locally settled the delivery
     * will not accept any additional updates to the {@link DeliveryState} via one of the
     * {@link #disposition(DeliveryState)} or {@link #disposition(DeliveryState, boolean)}
     * methods.
     *
     * @return this {@link IncomingDelivery} instance.
     */
    IncomingDelivery settle();

    /**
     * @return the {@link DeliveryState} at the remote side of this Delivery.
     */
    DeliveryState getRemoteState();

    /**
     * @return true if the delivery has been settled by the remote.
     */
    boolean isRemotelySettled();

    /**
     * Returns the total number of transfer frames that have occurred for the given {@link IncomingDelivery}.
     *
     * @return the number of {@link Transfer} frames that this {@link OutgoingDelivery} has initiated.
     */
    int getTransferCount();

    //----- Event handlers for the Incoming Delivery

    /**
     * Handler for incoming deliveries that is called for each incoming {@link Transfer} frame that comprises
     * either one complete delivery or a chunk of a split framed {@link Transfer}.  The handler should check
     * that the delivery being read is partial or not and act accordingly, as partial deliveries expect additional
     * updates as more frames comprising that {@link IncomingDelivery} arrive or the remote aborts the transfer.
     * <p>
     * This handler is useful in cases where an incoming delivery is split across many incoming {@link Transfer}
     * frames either due to a large size or a small max frame size setting and the processing is handed off to some
     * other resource other than the {@link Receiver} that original handling the first transfer frame.  If the initial
     * {@link Transfer} carries the entire delivery payload then this event handler will never be called.  Once set
     * this event handler receiver all updates of incoming delivery {@link Transfer} frames which would otherwise have
     * been sent to the {@link Receiver#deliveryReadHandler(EventHandler)} instance.
     *
     * @param handler
     *      The handler that will be invoked when {@link Transfer} frames arrive on this receiver link.
     *
     * @return this {@link IncomingDelivery} instance.
     */
    IncomingDelivery deliveryReadHandler(EventHandler<IncomingDelivery> handler);

    /**
     * Handler for aborted deliveries that is called if this delivery is aborted by the {@link Sender}.
     * <p>
     * This handler is an optional convenience handler that supplements the standard
     * {@link #deliveryReadHandler(EventHandler)} in cases where the users wishes to break out the
     * processing of inbound delivery data from abort processing.  If this handler is not set the
     * {@link Receiver} will call the registered {@link #deliveryAbortedHandler(EventHandler)}
     * if one is set.
     *
     * @param handler
     *      The handler that will be invoked when {@link Transfer} frames arrive on this receiver link.
     *
     * @return this {@link IncomingDelivery} instance.
     */
    IncomingDelivery deliveryAbortedHandler(EventHandler<IncomingDelivery> handler);

    /**
     * Handler for updates to the remote state of incoming deliveries that have previously been received.
     * <p>
     * Remote state updates for an {@link IncomingDelivery} can happen when the remote settles a complete
     * {@link IncomingDelivery} or otherwise modifies the delivery outcome and the user needs to act on those
     * changes such as a spontaneous update to the {@link DeliveryState}.  If the initial {@link Transfer} of
     * an incoming delivery already indicates settlement then this handler will never be called.
     *
     * @param handler
     *      The handler that will be invoked when a new remote state update for an {@link IncomingDelivery} arrives on this link.
     *
     * @return this {@link IncomingDelivery} instance.
     */
    IncomingDelivery deliveryStateUpdatedHandler(EventHandler<IncomingDelivery> handler);

}
