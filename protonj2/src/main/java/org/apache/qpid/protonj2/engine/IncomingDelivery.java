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
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * API for an incoming Delivery.
 */
public interface IncomingDelivery extends Delivery {

    /**
     * @return the link that this {@link Delivery} is bound to.
     */
    @Override
    Receiver getLink();

    /**
     * Returns the number of bytes currently available for reading form this delivery, which may not be complete yet.
     *
     * Note that this value will change as bytes are received, and is in general not equal to the total length of
     * a delivery, except the point where {@link #isPartial()} returns false and no content has yet been received by
     * the application.
     *
     * @return the number of bytes currently available to read from this delivery.
     */
    int available();

    // TODO - Pick names for these, receive, readBytes, take etc the old recv names weren't the greatest

    ProtonBuffer readAll();

    // TODO - Allow partial consumption or only support readAll (take) of buffer.

    IncomingDelivery readBytes(ProtonBuffer buffer);

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
