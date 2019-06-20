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

import java.util.Collection;
import java.util.function.Predicate;

import org.apache.qpid.proton4j.amqp.transport.DeliveryState;

/**
 * AMQP Receiver API
 */
public interface Receiver extends Link<Receiver> {

    /**
     * Sets the credit for the {@link Receiver} to the given amount.
     *
     * @param credit
     *      the new credit amount to set.
     *
     * @return this {@link Receiver}
     *
     * @throw {@link IllegalArgumentException} if the credit amount is negative.
     * @throw {@link IllegalStateException} if the Receiver is not open.
     */
    Receiver setCredit(int credit);

    // Receiver addCredit(int amount);
    // Receiver reduceCredit(int amount);

    /**
     * Initiate a drain of all remaining credit of this {@link Receiver} link.
     *
     * If the link has no credit then the drained event will be triggered immediately to
     * indicate that result.
     *
     * @return this {@link Receiver} for chaining.
     */
    Receiver drain();

    // Receiver drain(int credits);

    /**
     * Configures a default DeliveryState to be used if a received delivery is settled/freed
     * without any disposition state having been previously applied.
     *
     * @param state the default delivery state
     *
     * @return this {@link Receiver} for chaining.
     */
    public Receiver setDefaultDeliveryState(DeliveryState state);

    /**
     * @return the default delivery state for this delivery
     */
    public DeliveryState getDefaultDeliveryState();

    /**
     * For each unsettled outgoing delivery that is pending in the {@link Receiver} apply the given predicate
     * and if it matches then apply the given delivery state and settled value to it.
     *
     * @param filter
     *      The predicate to apply to each unsettled delivery to test for a match.
     * @param state
     *      The new {@link DeliveryState} to apply to any matching outgoing deliveries.
     * @param settle
     *      Boolean indicating if the matching unsettled deliveries should be settled.
     *
     * @return this {@link Receiver} for chaining
     */
    public Receiver disposition(Predicate<OutgoingDelivery> filter, DeliveryState state, boolean settle);

    /**
     * For each unsettled outgoing delivery that is pending in the {@link Receiver} apply the given predicate
     * and if it matches then settle the delivery.
     *
     * @param filter
     *      The predicate to apply to each unsettled delivery to test for a match.
     *
     * @return this {@link Receiver} for chaining
     */
    public Receiver settle(Predicate<OutgoingDelivery> filter);

    /**
     * Retrieves the list of unsettled deliveries for this {@link Receiver} link which have yet to be settled
     * on this end of the link.
     *
     * @return a collection of unsettled deliveries or an empty list if no pending deliveries are outstanding.
     */
    Collection<IncomingDelivery> unsettled();

    //----- Event handlers for the Receiver

    /**
     * When a drain is requested for this receiver an event handler will be signaled to indicate that
     * the drain was successful.
     *
     * TODO -- What to do when drain won't complete, another event or signal this one with async
     *         success or failure since it was asked for anyway async event isn't that unexpected.
     * Receiver receiverDrainedEventHandler(EventHandler<AsyncEvent<Receiver>> handler);
     *
     * @param handler
     *      The handler that will be invoked when receiver credit has been drained by the remote sender.
     *
     * @return this receiver
     */
    Receiver receiverDrainedEventHandler(EventHandler<Receiver> handler);

    /**
     * Handler for incoming deliveries
     *
     * @param handler
     *      The handler that will be invoked when a new delivery arrives on this receiver link.
     *
     * @return this receiver
     */
    Receiver deliveryReceivedEventHandler(EventHandler<IncomingDelivery> handler);

    /**
     * Handler for updates for deliveries that have previously been received.
     *
     * Updates can happen when the remote settles or otherwise modifies the delivery and the
     * user needs to act on those changes.
     *
     * @param handler
     *      The handler that will be invoked when a new update delivery arrives on this link.
     *
     * @return this receiver
     */
    Receiver deliveryUpdatedEventHandler(EventHandler<IncomingDelivery> handler);

}
