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

import org.apache.qpid.proton4j.amqp.DeliveryTag;
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
     * @return this {@link Sender} instance.
     *
     * @throws IllegalStateException if the link is not draining currently.
     */
    Sender drained();

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
     * @return this {@link Sender} instance.
     */
    Sender disposition(Predicate<OutgoingDelivery> filter, DeliveryState state, boolean settle);

    /**
     * For each unsettled outgoing delivery that is pending in the {@link Sender} apply the given predicate
     * and if it matches then settle the delivery.
     *
     * @param filter
     *      The predicate to apply to each unsettled delivery to test for a match.
     *
     * @return this {@link Sender} instance.
     */
    Sender settle(Predicate<OutgoingDelivery> filter);

    /**
     * Retrieves the list of unsettled deliveries sent from this {@link Sender}.  The deliveries in the {@link Collection}
     * cannot be written to but can have their settled state and disposition updated.  Only when this {@link Sender}
     * settles on its end are the {@link OutgoingDelivery} instances removed from the unsettled {@link Collection}.
     *
     * The {@link Collection} returned from this method is a copy of the internally maintained data and is
     * not modifiable.  The caller should use this method judiciously to avoid excess GC overhead.
     *
     * @return a collection of unsettled deliveries or an empty collection if no pending deliveries are outstanding.
     */
    Collection<OutgoingDelivery> unsettled();

    /**
     * @return true if there are unsettled deliveries for this {@link Sender} link.
     */
    boolean hasUnsettled();

    /**
     * Configures a {@link DeliveryTagGenerator} that will be used to create and set a {@link DeliveryTag}
     * value on each new {@link OutgoingDelivery} that is created and returned from the {@link Sender#next()}
     * method.
     *
     * @param generator
     *      The {@link DeliveryTagGenerator} to use to create automatic {@link DeliveryTag} values.
     *
     * @return this {@link Sender} instance.
     */
    Sender setDeliveryTagGenerator(DeliveryTagGenerator generator);

    /**
     * @return the currently configured {@link DeliveryTagGenerator} for this {@link Sender}.
     */
    DeliveryTagGenerator getDeliveryTagGenerator();

    //----- Event handlers for the Sender

    /**
     * Handler for updates for deliveries that have previously been sent.
     *
     * Updates can happen when the remote settles or otherwise modifies the delivery and the
     * user needs to act on those changes.
     *
     * @param handler
     *      The handler that will be invoked when a new update delivery arrives on this link.
     *
     * @return this {@link Sender} instance.
     */
    Sender deliveryStateUpdatedHandler(EventHandler<OutgoingDelivery> handler);

}
