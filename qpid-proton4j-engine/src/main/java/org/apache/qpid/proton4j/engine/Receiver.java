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
    // Receiver setCredit(int amount);

    // Receiver drain(EventHandler<Receiver> handler);
    // Receiver drain(int credits, EventHandler<Receiver> handler);

    //----- Event handlers for the Receiver

    /**
     * When a drain is requested for this receiver an event handler will be signaled to indicate that
     * the drain was successful.
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
