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

import java.util.Map;

import org.apache.qpid.protonj2.client.impl.ClientDeliveryState.ClientAccepted;
import org.apache.qpid.protonj2.client.impl.ClientDeliveryState.ClientModified;
import org.apache.qpid.protonj2.client.impl.ClientDeliveryState.ClientRejected;
import org.apache.qpid.protonj2.client.impl.ClientDeliveryState.ClientReleased;

/**
 * Conveys the outcome of a Delivery either incoming or outgoing.
 */
public interface DeliveryState {

    public enum Type {
        ACCEPTED,
        REJECTED,
        MODIFIED,
        RELEASED,
        TRANSACTIONAL
    }

    /**
     * @return the type that the given {@link DeliveryState} represents.
     */
    Type getType();

    /**
     * @return true if the {@link DeliveryState} represents an Accepted outcome.
     */
    default boolean isAccepted() {
        return getType() == Type.ACCEPTED;
    }

    //----- Factory methods for default DeliveryState types

    /**
     * @return an instance of an AMQP Accepted delivery outcome.
     */
    static DeliveryState accepted() {
        return ClientAccepted.getInstance();
    }

    /**
     * @return an instance of an AMQP Released delivery outcome.
     */
    static DeliveryState released() {
        return ClientReleased.getInstance();
    }

    /**
     * Create a new Rejected outcome with the given condition and description.
     *
     * @param condition
     * 		the condition that lead to the rejected outcome.
     * @param description
     * 		a meaningful description of the reason for the rejection.
     *
     * @return an instance of an AMQP Rejected delivery outcome.
     */
    static DeliveryState rejected(String condition, String description) {
        return new ClientRejected(condition, description);
    }

    /**
     * Create a new Rejected outcome with the given condition and description.
     *
     * @param condition
     * 		the condition that lead to the rejected outcome.
     * @param description
     * 		a meaningful description of the reason for the rejection.
     * @param info
     *      a {@link Map} containing additional information about the rejection.
     *
     * @return an instance of an AMQP Rejected delivery outcome.
     */
    static DeliveryState rejected(String condition, String description, Map<String, Object> info) {
        return new ClientRejected(condition, description, info);
    }

    /**
     * Create a new Modified outcome with the given failure state.
     *
     * @param failed
     * 		boolean indicating if delivery failed or not.
     * @param undeliverable
     * 		boolean indicating if the delivery cannot be sent to this receiver again.
     *
     * @return an instance of an AMQP Modified delivery outcome.
     */
    static DeliveryState modified(boolean failed, boolean undeliverable) {
        return new ClientModified(failed, undeliverable);
    }

    /**
     * Create a new Modified outcome with the given failure state.
     *
     * @param failed
     * 		boolean indicating if delivery failed or not.
     * @param undeliverable
     * 		boolean indicating if the delivery cannot be sent to this receiver again.
     * @param annotations
     *      updated annotation for the delivery that this outcome applies to.
     *
     * @return an instance of an AMQP Modified delivery outcome.
     */
    static DeliveryState modified(boolean failed, boolean undeliverable, Map<String, Object> annotations) {
        return new ClientModified(failed, undeliverable, annotations);
    }
}
