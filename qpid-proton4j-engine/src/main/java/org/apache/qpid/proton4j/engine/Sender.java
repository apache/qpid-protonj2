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
 * AMQP Sender API
 */
public interface Sender extends Link<Sender> {

    // OutgoingDelivery createDelivery();

    // boolean isSendable();

    // Sender drained(LinkCreditState state);

    //----- Event handlers for the Sender

    // Receiver sendableEventHandler(EventHandler<Sender> sender);

    // Receiver drainRequestedEventHandler(EventHandler<LinkCreditState> handler);

    /**
     * Handler for updates for deliveries that have previously been sent.
     *
     * Updates can happen when the remote settles or otherwise modifies the delivery and the
     * user needs to act on those changes.
     *
     * @param handler
     *      The handler that will be invoked when a new update delivery arrives on this link.
     *
     * @return this receiver
     */
    Receiver deliveryUpdatedEventHandler(EventHandler<OutgoingDelivery> handler);

}
