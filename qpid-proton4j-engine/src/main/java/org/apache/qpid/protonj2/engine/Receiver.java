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

import java.util.Collection;
import java.util.function.Predicate;

import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * AMQP Receiver API
 */
public interface Receiver extends Link<Receiver> {

    /**
     * Adds the given amount of credit for the {@link Receiver}.
     *
     * @param additionalCredit
     *      the new amount of credits to add.
     *
     * @return this {@link Receiver}
     *
     * @throws IllegalArgumentException if the credit amount is negative.
     */
    Receiver addCredit(int additionalCredit);

    /**
     * Initiate a drain of all remaining credit of this {@link Receiver} link.
     *
     * @return true if a drain was started or false if the link already had no credit to drain.
     *
     * @throws IllegalStateException if an existing drain attempt is incomplete.
     */
    boolean drain();

    /**
     * Initiate a drain of the given credit from this this {@link Receiver} link.  If the credit
     * given is greater than the current link credit the current credit is increased, however if
     * the amount of credit given is less that the current amount of link credit an exception is
     * thrown.
     *
     * @param credit
     *      The amount of credit that should be requested to be drained from this link.
     *
     * @return true if a drain was started or false if the value is zero and the link had no credit.
     *
     * @throws IllegalStateException if an existing drain attempt is incomplete.
     * @throws IllegalArgumentException if the credit value given is less than the current value.
     */
    boolean drain(int credit);

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
    public Receiver disposition(Predicate<IncomingDelivery> filter, DeliveryState state, boolean settle);

    /**
     * For each unsettled outgoing delivery that is pending in the {@link Receiver} apply the given predicate
     * and if it matches then settle the delivery.
     *
     * @param filter
     *      The predicate to apply to each unsettled delivery to test for a match.
     *
     * @return this {@link Receiver} for chaining
     */
    public Receiver settle(Predicate<IncomingDelivery> filter);

    /**
     * Retrieves the list of unsettled deliveries for this {@link Receiver} link which have yet to be settled
     * on this end of the link.  When the {@link IncomingDelivery} is settled by the receiver the value will
     * be removed from the collection.
     *
     * The {@link Collection} returned from this method is a copy of the internally maintained data and is
     * not modifiable.  The caller should use this method judiciously to avoid excess GC overhead.
     *
     * @return a collection of unsettled deliveries or an empty list if no pending deliveries are outstanding.
     */
    Collection<IncomingDelivery> unsettled();

    /**
     * @return true if there are unsettled deliveries for this {@link Receiver} link.
     */
    boolean hasUnsettled();

    //----- Event handlers for the Receiver

    /**
     * Handler for incoming deliveries that is called for each incoming {@link Transfer} frame that comprises
     * either one complete delivery or a chunk of a split framed {@link IncomingDelivery}.  The handler should
     * check that the delivery being read is partial or not and act accordingly, as there will be additional
     * updates as more frames comprising that {@link IncomingDelivery} arrive.
     *
     * @param handler
     *      The handler that will be invoked when delivery arrives on this receiver link.
     *
     * @return this receiver
     */
    Receiver deliveryReadHandler(EventHandler<IncomingDelivery> handler);

    /**
     * Handler for updates for deliveries that have previously been received.  Updates for an {@link IncomingDelivery}
     * can happen when the remote settles a complete {@link IncomingDelivery} or otherwise modifies the delivery outcome
     * and the user needs to act on those changes such as a spontaneous update to the {@link DeliveryState}.
     *
     * @param handler
     *      The handler that will be invoked when a new update delivery arrives on this link.
     *
     * @return this receiver
     */
    Receiver deliveryStateUpdatedHandler(EventHandler<IncomingDelivery> handler);

}
