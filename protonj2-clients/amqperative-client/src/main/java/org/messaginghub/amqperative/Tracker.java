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
package org.messaginghub.amqperative;

import java.util.concurrent.Future;

import org.apache.qpid.proton4j.amqp.DeliveryTag;

/**
 * Tracker object used to track the state of a sent {@link Message} at the remote
 * and allows for local settlement and disposition management.
 */
public interface Tracker {

    // TODO Think about if we want to be able to return the original message from
    //      a tracker which while nice has some definite complications such as if
    //      the user happens to be sending the same message over and over with small
    //      modifications.  JMS style rules are annoying but would need something that
    //      give that sort of direction on when you can do this.

    /**
     * @return the {@link Sender} that was used to send the delivery that is being tracked.
     */
    Sender sender();

    /**
     * Gets the delivery tag for this delivery
     *
     * @return the binary delivery tag that has been assigned to the sent message.
     */
    DeliveryTag tag();

    /**
     * Settles the delivery locally, if not {@link SenderOptions#autoSettle() auto-settling}.
     *
     * @return the delivery
     */
    Tracker settle();

    /**
     * @return true if the sent message has been locally settled.
     */
    boolean settled();

    /**
     * Gets the current local state for the tracked delivery.
     *
     * @return the delivery state
     */
    DeliveryState localState();

    /**
     * Gets the current remote state for the tracked delivery.
     *
     * @return the {@link Future} that will be completed when a remote delivery state is applied.
     */
    Future<DeliveryState> remoteState();

    /**
     * Gets whether the delivery was settled by the remote peer yet.
     *
     * @return whether the delivery is remotely settled
     */
    boolean remotelySettled();

    /**
     * Accepts and settles the delivery.
     *
     * @return itself
     */
    Tracker accept();

    /**
     * Updates the DeliveryState, and optionally settle the delivery as well.
     *
     * @param state
     *            the delivery state to apply
     * @param settle
     *            whether to {@link #settle()} the delivery at the same time
     *
     * @return itself
     */
    Tracker disposition(DeliveryState state, boolean settle);

}
