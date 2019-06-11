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

/**
 * AMQP Sender API
 */
public interface Sender extends Link<Sender> {

    // int available();

    // boolean isDraining();

    // boolean isDrained();

    // Sender drained(LinkCreditState state);

    // Sender disposition(OutgoingDelivery... deliveries, DeliveryState state, boolean settle);

    // Sender settle(OoutgoingDelivery... deliveries);

    /**
     * Checks if the sender has credit and the session window allows for any bytes to be written currently.
     *
     * @return true if the link has credit and the session window allows for any bytes to be written.
     */
    boolean isSendable();

    /**
     * Gets the current incomplete {@link OutgoingDelivery} or creates a new instance for use by the
     * sender.  An {@link OutgoingDelivery} must be marked completed before a new instance will be returned
     * from this method.
     *
     * @return the current active outgoing delivery or a new instance ready for sending.
     */
    OutgoingDelivery current();

    // TODO - Sample method for accessing link unsettled deliveries from the API level view
    //        Another option might be an foreach style method that allows a consumer to be applied
    //        or a disposition method that takes a predicate and the state to be applied.
    // public void disposition(Predicate<OutgoingDelivery> filter, DeliveryState state, boolean settle);
    // public void settle(Predicate<OutgoingDelivery> filter);

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
    Sender sendableEventHandler(EventHandler<Sender> handler);

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
    Sender drainRequestedEventHandler(EventHandler<LinkCreditState> handler);

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
    Sender deliveryUpdatedEventHandler(EventHandler<OutgoingDelivery> handler);

}
