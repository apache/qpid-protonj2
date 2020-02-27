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
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * AMQP Sender API
 */
public interface Sender extends Link<Sender> {

    /**
     * Called when the {@link Receiver} has requested a drain of credit and the sender
     * has sent all available messages.
     *
     * @param state
     *      The {@link Link} credit state that was given when the sender drain started.
     *
     * @return this Sender instance for chaining.
     */
    Sender drained(LinkCreditState state);

    /**
     * Checks if the sender has credit and the session window allows for any bytes to be written currently.
     *
     * @return true if the link has credit and the session window allows for any bytes to be written.
     */
    boolean isSendable();

    /**
     * Gets the current {@link OutgoingDelivery} for this {@link Sender} if one is available.
     * <p>
     * The sender only tracks a current delivery in the case that the next method has bee called
     * and if any bytes are written to the delivery using the streaming based API
     * {@link OutgoingDelivery#streamBytes(ProtonBuffer)} which allows for later writing of additional
     * bytes to the delivery.  Once the method {@link OutgoingDelivery#writeBytes(ProtonBuffer)} is
     * called the final {@link Transfer} is written indicating that the delivery is complete and the
     * current delivery value is reset.  An outgoing delivery that is being streamed may also
     * be completed by calling the {@link OutgoingDelivery#abort()} method.
     *
     * @return the current active outgoing delivery or null if there is no current delivery.
     */
    OutgoingDelivery current();

    /**
     * When there has been no deliveries so far or the current delivery has reached a complete state this
     * method updates the current delivery to a new instance and returns that value.  If the current
     * {@link OutgoingDelivery} has not been completed by either calling the
     * {@link OutgoingDelivery#writeBytes(ProtonBuffer)} or the {@link OutgoingDelivery#abort()} method then
     * this method will throw an exception to indicate the sender state cannot allow a new delivery to be started.
     *
     * @return a new delivery instance unless the current delivery is not complete.
     *
     * @throws IllegalStateException if the current delivery has not been marked complete.
     */
    OutgoingDelivery next();

    /**
     * For each unsettled outgoing delivery that is pending in the {@link Sender} apply the given predicate
     * and if it matches then apply the given delivery state and settled value to it.
     *
     * @param filter
     *      The predicate to apply to each unsettled delivery to test for a match.
     * @param state
     *      The new {@link DeliveryState} to apply to any matching outgoing deliveries.
     * @param settle
     *      Boolean indicating if the matching unsettled deliveries should be settled.
     *
     * @return this sender for chaining.
     */
    public Sender disposition(Predicate<OutgoingDelivery> filter, DeliveryState state, boolean settle);

    /**
     * For each unsettled outgoing delivery that is pending in the {@link Sender} apply the given predicate
     * and if it matches then settle the delivery.
     *
     * @param filter
     *      The predicate to apply to each unsettled delivery to test for a match.
     *
     * @return this sender for chaining.
     */
    public Sender settle(Predicate<OutgoingDelivery> filter);

    /**
     * Retrieves the list of unsettled deliveries sent from this {@link Sender}.  The deliveries in the list
     * cannot be written to but can have their settled state and disposition updated.
     *
     * @return a collection of unsettled deliveries or an empty list if no pending deliveries are outstanding.
     */
    Collection<OutgoingDelivery> unsettled();

    //----- Event handlers for the Sender

    /**
     * Handler for updates on this {@link Sender} that indicates that an event has occurred that has
     * placed this sender in a state where a send is possible.
     *
     * @param handler
     *      An event handler that will be signaled when the link state changes to allow sends.
     *
     * @return this sender
     */
    Sender sendableHandler(EventHandler<Sender> handler);

    /**
     * Called when the {@link Receiver} end of the link has requested a drain of the outstanding
     * credit for this {@link Sender}.
     *
     * The drain request is accompanied by the current credit state for this link which the application
     * must hand back to the {@link Sender} when it has completed the drain request.
     *
     * @param handler
     *      handler that will act on the drain request.
     *
     * @return this sender
     */
    Sender drainRequestedHandler(EventHandler<LinkCreditState> handler);

    /**
     * Handler for updates for deliveries that have previously been sent.
     *
     * Updates can happen when the remote settles or otherwise modifies the delivery and the
     * user needs to act on those changes.
     *
     * @param handler
     *      The handler that will be invoked when a new update delivery arrives on this link.
     *
     * @return this sender
     */
    Sender deliveryUpdatedHandler(EventHandler<OutgoingDelivery> handler);

}
