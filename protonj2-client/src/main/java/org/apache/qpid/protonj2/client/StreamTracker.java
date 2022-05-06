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

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.exceptions.ClientDeliveryStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.transport.Disposition;

/**
 * Special StreamSender related {@link Tracker} that is linked to any {@link StreamSenderMessage}
 * instance and provides the {@link Tracker} functions for those types of messages.
 */
public interface StreamTracker {

    /**
     * @return the {@link StreamSender} that was used to send the delivery that is being tracked.
     */
    StreamSender sender();

    /**
     * Settles the delivery locally, if not {@link SenderOptions#autoSettle() auto-settling}.
     *
     * @return this {@link Tracker} instance.
     *
     * @throws ClientException if an error occurs while performing the settlement.
     */
    StreamTracker settle() throws ClientException;

    /**
     * @return true if the sent message has been locally settled.
     */
    boolean settled();

    /**
     * Gets the current local state for the tracked delivery.
     *
     * @return the delivery state
     */
    DeliveryState state();

    /**
     * Gets the current remote state for the tracked delivery.
     *
     * @return the remote {@link DeliveryState} once a value is received from the remote.
     */
    DeliveryState remoteState();

    /**
     * Gets whether the delivery was settled by the remote peer yet.
     *
     * @return whether the delivery is remotely settled
     */
    boolean remoteSettled();

    /**
     * Updates the DeliveryState, and optionally settle the delivery as well.
     *
     * @param state
     *            the delivery state to apply
     * @param settle
     *            whether to {@link #settle()} the delivery at the same time
     *
     * @return this {@link Tracker} instance.
     *
     * @throws ClientException if an error occurs while applying the given disposition
     */
    StreamTracker disposition(DeliveryState state, boolean settle) throws ClientException;

    /**
     * Returns a future that can be used to wait for the remote to acknowledge receipt of
     * a sent message by settling it.
     *
     * @return a {@link Future} that can be used to wait on remote settlement.
     */
    Future<StreamTracker> settlementFuture();

    /**
     * Waits if necessary for the remote to settle the sent delivery unless it has
     * either already been settled or the original delivery was sent settled in which
     * case the remote will not send a {@link Disposition} back.
     *
     * @return this {@link Tracker} instance.
     *
     * @throws ClientException if an error occurs while awaiting the remote settlement.
     */
    StreamTracker awaitSettlement() throws ClientException;

    /**
     * Waits if necessary for the remote to settle the sent delivery unless it has
     * either already been settled or the original delivery was sent settled in which
     * case the remote will not send a {@link Disposition} back.
     *
     * @param timeout
     *      the maximum time to wait for the remote to settle.
     * @param unit
     *      the time unit of the timeout argument.
     *
     * @return this {@link Tracker} instance.
     *
     * @throws ClientException if an error occurs while awaiting the remote settlement.
     */
    StreamTracker awaitSettlement(long timeout, TimeUnit unit) throws ClientException;

    /**
     * Waits if necessary for the remote to settle the sent delivery with an {@link Accepted}
     * disposition unless it has either already been settled and accepted or the original delivery
     * was sent settled in which case the remote will not send a {@link Disposition} back.
     *
     * @return this {@link Tracker} instance.
     *
     * @throws ClientDeliveryStateException if the remote sends a disposition other than Accepted.
     * @throws ClientException if an error occurs while awaiting the remote settlement.
     */
    StreamTracker awaitAccepted() throws ClientException;

    /**
     * Waits if necessary for the remote to settle the sent delivery with an {@link Accepted}
     * disposition unless it has either already been settled and accepted or the original delivery
     * was sent settled in which case the remote will not send a {@link Disposition} back.
     *
     * @param timeout
     *      the maximum time to wait for the remote to settle.
     * @param unit
     *      the time unit of the timeout argument.
     *
     * @return this {@link Tracker} instance.
     *
     * @throws ClientDeliveryStateException if the remote sends a disposition other than Accepted.
     * @throws ClientException if an error occurs while awaiting the remote settlement.
     */
    StreamTracker awaitAccepted(long timeout, TimeUnit unit) throws ClientException;

}
